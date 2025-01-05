import sys
import time
import requests
from requests.exceptions import ConnectTimeout, ReadTimeout
from urllib.parse import urlsplit, urljoin, unquote
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
import os
from enum import Enum


class ContentTypeException(Exception):
    pass


class ContentType(Enum):
    HTML = "text/html"
    XML = "application/xml"
    TXT = "text/plain"


class WebCrawler:
    USER_AGENT = "SantaBot"
    FROM_EMAIL = "test@email.com"

    # Consecutive fetch limits
    NETLOC_CONSECUTIVE_FETCH_PAUSE_TRIGGER = 5  # How many consecutive fetches before pausing netloc
    NETLOC_CONSECUTIVE_FETCH_PAUSE_SEC = 60  # Seconds to pause netloc after reaching limit

    # Netloc timeout handling limits
    NETLOC_CONSECUTIVE_TIMEOUT_PAUSE_TRIGGER = NETLOC_CONSECUTIVE_FETCH_PAUSE_TRIGGER  # How many consecutive timeouts within a netloc before pausing it (must be <= NETLOC_CONSECUTIVE_FETCH_PAUSE_TRIGGER)
    NETLOC_CONSECUTIVE_TIMEOUT_INITIAL_PAUSE_SEC = 60  # Seconds to pause netloc before it can be retried (initial value of exponential backoff)

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
        ".css", ".js", ".json", ".xml", ".csv", ".txt",
    }

    def __init__(self, initial_urls, html_limit):
        self.frontier_q = initial_urls
        self.visited = set()

        self.html_count = 0
        self.html_limit = html_limit

        self.last_fetch_netloc = ""
        self.last_fetch_timeout = False
        self.url_fetch_history = [] # Keep NETLOC_CONSECUTIVE_TIMEOUT_PAUSE_TRIGGER previous urls fetched to requeue after consecutive timeout

        self.netloc_seen = {}
        self.netloc_rp = {}

        self.netloc_pause_until = {}  # Timestamp when netloc can be retried

        self.netloc_consecutive_timeout_count = {}  # Count consecutive timeouts per netloc
        self.netloc_consecutive_timeout_pause_count = {}  # How many times netloc has been paused due to consecutive timeouts

        self.netloc_consecutive_fetch_count = {}  # Count consecutive fetches per netloc
    
    def save_url_fetch_history(self, url):
        self.url_fetch_history.insert(0, url)
        if len(self.url_fetch_history) > self.NETLOC_CONSECUTIVE_TIMEOUT_PAUSE_TRIGGER:
            self.url_fetch_history.pop()

    def requeue_url_fetch_history(self):
        for url in self.url_fetch_history:
            self.frontier_q.insert(0, url)
            try:
                self.visited.remove(url)
            except KeyError:
                pass

    def get_raw_document(self, url, content_type):
        headers = {"User-Agent": self.USER_AGENT, "From": self.FROM_EMAIL}
        response = requests.get(url, headers=headers, timeout=5)
        response.raise_for_status()
        header_content_type = response.headers["content-type"]
        if not content_type.value in header_content_type:
            raise ContentTypeException
        if content_type == ContentType.HTML:
            soup = BeautifulSoup(response.content, "html.parser")
            return soup.prettify().lower()
        else:
            return response.text

    def get_raw_urls_in_page(self, raw_html):
        soup = BeautifulSoup(raw_html, "html.parser")
        return [a_tag["href"] for a_tag in soup.find_all("a", href=True)]

    def get_normalized_urls(self, base_url, urls):
        base_url_parts = urlsplit(base_url)
        base_url = f"{base_url_parts.scheme}://{base_url_parts.netloc}"
        return [unquote(urljoin(base_url, url)) for url in urls]

    def write_file(self, path, text):
        dirname, filename = os.path.split(path)
        if dirname:
            os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)

    def append_file(self, path, text):
        dirname, filename = os.path.split(path)
        if dirname:
            os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a+", encoding="utf-8") as f:
            f.write(text)

    def filter_and_enqueue_urls(self, urls):
        for url in urls:
            url_parts = urlsplit(url)
            if (
                url not in self.frontier_q
                and url not in self.visited
                and url_parts.scheme.startswith("http")
                and url_parts.netloc.endswith(".ku.ac.th")
                and not url_parts.path.endswith(tuple(self.EXCLUDED_EXTENSIONS))
            ):
                self.frontier_q.append(url)

    def dequeue_url(self):
        current_time = time.time()
        for i in range(len(self.frontier_q)):
            url = self.frontier_q[i]
            netloc = urlsplit(url).netloc

            # Skip url if its netloc is paused
            if netloc in self.netloc_pause_until and current_time < self.netloc_pause_until[netloc]:
                continue

            return self.frontier_q.pop(i)

        return ""

    def get_html_file_path(self, url):
        url_parts = urlsplit(url)
        path_part = url_parts.path.strip("/").replace(".htm", ".html")
        if ".html" not in path_part:
            path_part = "page.html" if path_part == "" else f"{path_part}/page.html"
        path_part = path_part.replace(".html", "")
        query_part = url_parts.query.replace("?", "_").replace("&", "_")
        fragment_part = url_parts.fragment

        html_file_path = f"html/{url_parts.netloc}/{path_part}"
        if query_part:
            html_file_path += f"_{query_part}"
        if fragment_part:
            html_file_path += f"_#{fragment_part}"
        return f"{html_file_path}.html"

    def try_get_and_parse_robots_txt(self, netloc):
        if netloc not in self.netloc_seen:
            self.netloc_seen[netloc] = True
            robots_txt_url = f"https://{netloc}/robots.txt"
            try:
                robots_txt = self.get_raw_document(robots_txt_url, ContentType.TXT)

                # Below is only executed if the robots.txt is successfully fetched
                print(f"Found robots.txt at {robots_txt_url}")

                rp = RobotFileParser()
                rp.parse(robots_txt.splitlines())
                self.netloc_rp[netloc] = rp

                robots_txt_file_path = f"html/{netloc}/robots.txt"
                self.write_file(robots_txt_file_path, robots_txt)
                self.append_file("list_robots.txt", f"{netloc}\n")

                if rp.site_maps():
                    print(f"Found sitemap at {rp.site_maps()}")
                    self.append_file("list_sitemap.txt", f"{netloc}\n")

            except (Exception) as e:
                print(f"Failed to get or parse robots.txt for {netloc}: {e}")

    def crawl(self):
        start_time = time.time()

        while len(self.frontier_q) > 0 and self.html_count < self.html_limit:
            current_url = self.dequeue_url()
            if not current_url:
                time.sleep(1)
                continue
            self.visited.add(current_url)

            current_url_parts = urlsplit(current_url)
            self.try_get_and_parse_robots_txt(current_url_parts.netloc)
            if (
                current_url_parts.netloc in self.netloc_rp
                and not self.netloc_rp[current_url_parts.netloc].can_fetch(self.USER_AGENT, current_url)
            ):
                continue

            self.process_url(current_url)
            time.sleep(1)

        self.print_completion_time(start_time)

    def handle_netloc_consecutive_fetch(self, current_netloc):
        if not current_netloc in self.netloc_consecutive_fetch_count:
            self.netloc_consecutive_fetch_count[current_netloc] = 0

        if self.last_fetch_netloc == current_netloc:
            self.netloc_consecutive_fetch_count[current_netloc] += 1
        else:
            self.netloc_consecutive_fetch_count[current_netloc] = 1
            if self.last_fetch_netloc:
                self.netloc_consecutive_fetch_count[self.last_fetch_netloc] = 0

        if self.netloc_consecutive_fetch_count[current_netloc] == self.NETLOC_CONSECUTIVE_FETCH_PAUSE_TRIGGER:
            self.netloc_pause_until[current_netloc] = time.time() + self.NETLOC_CONSECUTIVE_FETCH_PAUSE_SEC
            print(f"{current_netloc} has exceeded max consecutive fetch count. Pausing for {self.NETLOC_CONSECUTIVE_FETCH_PAUSE_SEC} seconds")

    def process_url(self, current_url):
        current_netloc = urlsplit(current_url).netloc

        if not current_netloc in self.netloc_consecutive_timeout_count:
            self.netloc_consecutive_timeout_count[current_netloc] = 0
        if not current_netloc in self.netloc_consecutive_timeout_pause_count:
            self.netloc_consecutive_timeout_pause_count[current_netloc] = 0

        current_fetch_timeout = False
        try:
            self.save_url_fetch_history(current_url)
            self.handle_netloc_consecutive_fetch(current_netloc)
            raw_html = self.get_raw_document(current_url, ContentType.HTML)

            # Below is only executed if the html is successfully fetched
            html_file_path = self.get_html_file_path(current_url)
            self.write_file(html_file_path, raw_html)
            self.html_count += 1
            print(f"#{self.html_count} Got html from {current_url}")

            self.netloc_consecutive_timeout_count[current_netloc] = 0
            self.netloc_consecutive_timeout_count[self.last_fetch_netloc] = 0
            self.netloc_consecutive_timeout_pause_count[current_netloc] = 0

            raw_urls_in_page = self.get_raw_urls_in_page(raw_html)
            normalized_urls = self.get_normalized_urls(current_url, raw_urls_in_page)
            old_frontier_q_size = len(self.frontier_q)
            self.filter_and_enqueue_urls(normalized_urls)
            print(f"    Found {len(self.frontier_q) - old_frontier_q_size} new urls ({len(self.frontier_q)} total)")

        except (ConnectTimeout, ReadTimeout):
            current_fetch_timeout = True

            # Handle netloc consecutive timeout
            if current_netloc == self.last_fetch_netloc:
                self.netloc_consecutive_timeout_count[current_netloc] += 1
            else:
                self.netloc_consecutive_timeout_count[current_netloc] = 1
                if self.last_fetch_netloc:
                    self.netloc_consecutive_timeout_count[self.last_fetch_netloc] = 0
            
            if self.netloc_consecutive_timeout_count[current_netloc] == self.NETLOC_CONSECUTIVE_TIMEOUT_PAUSE_TRIGGER:
                self.netloc_consecutive_timeout_pause_count[current_netloc] += 1
                self.netloc_pause_until[current_netloc] = time.time() + self.NETLOC_CONSECUTIVE_TIMEOUT_INITIAL_PAUSE_SEC * 2 ** (self.netloc_consecutive_timeout_pause_count[current_netloc] - 1)
                self.requeue_url_fetch_history()
                print(f"{current_netloc} has exceeded max consecutive timeout count. Pausing for {self.NETLOC_CONSECUTIVE_TIMEOUT_INITIAL_PAUSE_SEC} seconds")

        except Exception as e:
            print(f"Error processing URL {current_url}: {e}")

        finally:
            self.last_fetch_netloc = current_netloc
            if current_fetch_timeout:
                self.last_fetch_timeout = True
            else:
                self.last_fetch_timeout = False

    def print_completion_time(self, start_time):
        end_time = time.time()
        hours, seconds = divmod(end_time - start_time, 3600)
        minutes, seconds = divmod(seconds, 60)
        print(f"Time taken: {hours} hours, {minutes} minutes, {seconds} seconds")


def main():
    initial_urls = [
        "https://www.ku.ac.th/th/",
        "https://www.ku.ac.th/th/faculty-bangkhen",
        "https://www.ku.ac.th/th/faculty-kamphaeng-saen-campus",
        "https://www.ku.ac.th/th/faculty-chalermphakiet-campus-sakon-nakhon",
        "https://www.ku.ac.th/th/faculty-sriracha-campus",
        "https://www.ku.ac.th/th/faculty-suphanburi-campus-establishment-project/",
        "https://www.ku.ac.th/th/faculty-associate-institution",
    ]

    html_limit = 10000
    if len(sys.argv) > 1:
        try:
            html_limit = int(sys.argv[1])
        except ValueError:
            print("Invalid argument for html limit")
            sys.exit(1)
    print(f"Crawling {html_limit} html pages")

    crawler = WebCrawler(initial_urls, html_limit)
    crawler.crawl()


if __name__ == "__main__":
    main()
