import requests
from requests.exceptions import HTTPError, ConnectTimeout
from urllib.parse import urlsplit, urljoin, unquote
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
import os
import asyncio


class ContentTypeException(Exception):
    pass


CONTENT_TYPE_HTML = "text/html"
CONTENT_TYPE_XML = "application/xml"
CONTENT_TYPE_TXT = "text/plain"


USER_AGENT = "SantaBot"


def get_raw_document(url, content_type):
    headers = {"User-Agent": USER_AGENT, "From": "test@email.com"}
    response = requests.get(url, headers=headers, timeout=5)
    response.raise_for_status()
    header_content_type = response.headers["content-type"]
    if not content_type in header_content_type:
        raise ContentTypeException
    if content_type == CONTENT_TYPE_HTML:
        soup = BeautifulSoup(response.content, "html.parser")
        return soup.prettify().lower()
    else:
        return response.text


# def get_raw_urls(raw_html):
#     urls = []
#     pattern_double_quotes = ('<a href="', '"')
#     pattern_single_quotes = ('<a href=\'', '\'')
#     index = 0;  length = len(raw_html)
#     while index < length:
#         start_tag_single_quotes = raw_html.find(pattern_single_quotes[0], index)
#         start_tag_double_quotes = raw_html.find(pattern_double_quotes[0], index)
#         current_is_double_quotes = False
#         start_url = 0; end_url = 0

#         if start_tag_double_quotes < 0 and start_tag_single_quotes < 0:
#             break
#         if start_tag_single_quotes < 0:
#             current_is_double_quotes = True
#         elif start_tag_double_quotes < 0:
#             current_is_double_quotes = False
#         elif start_tag_double_quotes < start_tag_single_quotes:
#             current_is_double_quotes = True
#         else:
#             current_is_double_quotes = False

#         if current_is_double_quotes:
#             start_url = start_tag_double_quotes + len(pattern_double_quotes[0])
#             end_url = raw_html.find(pattern_double_quotes[1], start_url)
#         else:
#             start_url = start_tag_single_quotes + len(pattern_single_quotes[0])
#             end_url = raw_html.find(pattern_single_quotes[1], start_url)

#         link = raw_html[start_url:end_url]
#         if len(link) > 0:
#             if link not in urls:
#                 urls.append(link)

#         index = end_url + 1

#     return urls


def get_raw_urls(raw_html):
    soup = BeautifulSoup(raw_html, "html.parser")
    urls = []
    for a_tag in soup.find_all("a", href=True):
        urls.append(a_tag["href"])
    return urls


def get_normalized_urls(base_url, urls):
    base_url_parts = urlsplit(base_url)
    base_url = f"{base_url_parts.scheme}://{base_url_parts.netloc}"
    normalized_urls = []
    for url in urls:
        normalized_urls.append(unquote(urljoin(base_url, url)))
    return normalized_urls


def write_file(path, text):
    dirname, filename = os.path.split(path)
    if dirname:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def append_file(path, text):
    dirname, filename = os.path.split(path)
    if dirname:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a+", encoding="utf-8") as f:
        f.write(text)


frontier_q = [
    "https://www.ku.ac.th/th/",
    "https://www.eng.ku.ac.th/",
    "https://cpe.ku.ac.th/",
]
visited = set()

netloc_consecutive_fetch_count = {}
# used to limit consecutive fetch from the same netloc
NETLOC_CONSECUTIVE_FETCH_LIMIT = 3
# pause time in seconds after reaching the limit
NETLOC_CONSECUTIVE_FETCH_PAUSE_SEC = 15

EXCLUDED_EXTENSIONS = {
    # Images
    ".jpg", ".jpeg", ".png", ".svg", ".gif", ".webp", ".bmp", ".tiff",
    # Documents
    ".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx",
    # Archives
    ".zip", ".rar", ".tar", ".gz", ".7z", ".iso",
    # Media
    ".mp3", ".wav", ".ogg", ".mp4", ".avi", ".mov",
    # Executables and scripts
    ".exe", ".dll", ".bin", ".bat", ".sh",
    # Other common file types
    ".css", ".js", ".json", ".xml", ".csv", ".txt",  # fmt: skip
}


async def wait_reset_consecutive_fetch(netloc, pause_sec):
    await asyncio.sleep(pause_sec)
    netloc_consecutive_fetch_count[netloc] = 0


def filter_and_enqueue_urls(urls):
    for url in urls:
        url_parts = urlsplit(url)
        if (
            url not in frontier_q
            and url not in visited
            and url_parts.scheme.startswith("http")
            and url_parts.netloc.endswith(".ku.ac.th")
            and not url_parts.path.endswith(tuple(EXCLUDED_EXTENSIONS))
        ):
            frontier_q.append(url)


# FIFO, BFS
async def dequeue_url():
    current_url = ""
    i = 0
    for i in range(len(frontier_q)):
        url = frontier_q[i]
        netloc = urlsplit(url).netloc
        if netloc_consecutive_fetch_count.get(netloc) is None:
            netloc_consecutive_fetch_count[netloc] = 0

        if netloc_consecutive_fetch_count[netloc] < NETLOC_CONSECUTIVE_FETCH_LIMIT:
            current_url = frontier_q.pop(i)
            return current_url, i
    else:
        return "", -1


def get_html_file_path(url):
    url_parts = urlsplit(url)
    path_part = url_parts.path.strip("/").replace(".htm", ".html")
    if ".html" not in path_part:
        if path_part == "":
            path_part = "page.html"
        else:
            path_part += "/page.html"
    path_part = path_part.replace(".html", "")
    query_part = url_parts.query.replace("?", "_").replace("&", "_")
    fragment_part = url_parts.fragment
    html_file_path = f"html/{url_parts.netloc}/{path_part}"
    if query_part != "":
        html_file_path += f"_{query_part}"
    if fragment_part != "":
        html_file_path += f"_{fragment_part}"
    html_file_path += ".html"
    return html_file_path


async def main():
    netloc_seen = {}
    netloc_rp = {}
    html_count = 0
    last_fetch_netloc = ""

    while len(frontier_q) > 0 and html_count < 10000:
        current_url, pos_in_frontier = await dequeue_url()
        if current_url == "":
            await asyncio.sleep(1)
            continue
        visited.add(current_url)
        current_url_parts = urlsplit(current_url)

        if netloc_consecutive_fetch_count[current_url_parts.netloc] is None:
            netloc_consecutive_fetch_count[current_url_parts.netloc] = 0

        if last_fetch_netloc == current_url_parts.netloc:
            netloc_consecutive_fetch_count[current_url_parts.netloc] += 1
        else:
            netloc_consecutive_fetch_count[current_url_parts.netloc] = 1
            if (
                last_fetch_netloc != ""
                and netloc_consecutive_fetch_count[last_fetch_netloc]
                < NETLOC_CONSECUTIVE_FETCH_LIMIT
            ):
                netloc_consecutive_fetch_count[last_fetch_netloc] = 0

        if (
            netloc_consecutive_fetch_count[current_url_parts.netloc]
            == NETLOC_CONSECUTIVE_FETCH_LIMIT
        ):
            asyncio.create_task(
                wait_reset_consecutive_fetch(
                    current_url_parts.netloc, NETLOC_CONSECUTIVE_FETCH_PAUSE_SEC
                )
            )

        # prepare robots.txt
        if current_url_parts.netloc not in netloc_seen:
            netloc_seen[current_url_parts.netloc] = True
            robots_txt_url = ""
            try:
                robots_txt_url = f"{current_url_parts.scheme}://{current_url_parts.netloc}/robots.txt"
                robots_txt = get_raw_document(robots_txt_url, CONTENT_TYPE_TXT)

                # below will only be executed if robots.txt is found
                rp = RobotFileParser()
                rp.parse(robots_txt.splitlines())
                netloc_rp[current_url_parts.netloc] = rp

                # write robots.txt to file
                robots_txt_file_path = f"html/{current_url_parts.netloc}/robots.txt"
                write_file(robots_txt_file_path, robots_txt)

                # save netloc to file if the it has robots.txt and sitemap
                print(f"Found robots.txt at {robots_txt_url}")
                append_file("list_robots.txt", current_url_parts.netloc + "\n")
                if rp.site_maps():
                    print(f"Found sitemap at {rp.site_maps()}")
                    append_file("list_sitemap.txt", current_url_parts.netloc + "\n")

            except ContentTypeException:
                print(f"{robots_txt_url} is not of content-type {CONTENT_TYPE_TXT}")
            except HTTPError as http_err:
                print(f"HTTP error occurred: {http_err}")
            except ConnectTimeout as timeout_err:
                print(f"Timeout error occurred: {timeout_err}")
            except Exception as err:
                print(f"Other error occurred: {err}")

        # do not fetch if prohibited in robots.txt, fetch if there isn't one
        if netloc_rp.get(current_url_parts.netloc) and not netloc_rp[
            current_url_parts.netloc
        ].can_fetch(USER_AGENT, current_url):
            continue

        try:
            await asyncio.sleep(1)
            raw_html = get_raw_document(current_url, CONTENT_TYPE_HTML)

            # below will only be executed if the url returns html
            # write html to file
            html_file_path = get_html_file_path(current_url)
            write_file(html_file_path, raw_html)

            html_count += 1
            print(f"(#{html_count}) Found html at {current_url}")

            # extract urls within the page
            raw_urls = get_raw_urls(raw_html)
            normalized_urls = get_normalized_urls(current_url, raw_urls)
            filter_and_enqueue_urls(normalized_urls)
            print(f"Current frontier size: {len(frontier_q)}")

        except ContentTypeException:
            print(f"{current_url} is not of content-type {CONTENT_TYPE_HTML}")
        except HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except ConnectTimeout as timeout_err:
            print(f"Timeout error occurred: {timeout_err}")
        except Exception as err:
            print(f"Other error occurred: {err}")
        finally:
            last_fetch_netloc = current_url_parts.netloc


asyncio.run(main())
