import sys
import time
import requests
from requests.exceptions import ConnectTimeout, ReadTimeout
from urllib.parse import urlsplit, urljoin, unquote
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
import os
from enum import Enum
import pickle
import signal
import argparse


class ContentTypeException(Exception):
    def __init__(self, expected_content_type, actual_content_type):
        self.expected_content_type = expected_content_type
        self.actual_content_type = actual_content_type

    def __str__(self):
        return f"Expected content type: {self.expected_content_type}, actual content type: {self.actual_content_type}"


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

    def __init__(self, initial_urls, html_limit, netloc_page_limit, pickle_file_path=None):
        if pickle_file_path and os.path.exists(pickle_file_path):
            self.load_state(pickle_file_path)
        else:
            self.frontier_q = initial_urls
            self.visited = set()

            self.HTML_LIMIT = html_limit
            self.html_count = 0

            self.NETLOC_PAGE_LIMIT = netloc_page_limit
            self.netloc_page_count = {}

            self.url_fetch_history = []
            self.last_fetch_timeout = False

            self.netloc_seen = {}
            self.netloc_rp = {}

            self.netloc_pause_until = {}
            self.netloc_consecutive_timeout_count = {}
            self.netloc_consecutive_timeout_pause_count = {}
            self.netloc_consecutive_fetch_count = {}

    def save_state(self):
        """Save crawler state to a single pickle file"""
        
        state = {
            'frontier_q': self.frontier_q,
            'visited': self.visited,
            'HTML_LIMIT': self.HTML_LIMIT,
            'html_count': self.html_count,
            'NETLOC_PAGE_LIMIT': self.NETLOC_PAGE_LIMIT,
            'netloc_page_count': self.netloc_page_count,
            'url_fetch_history': self.url_fetch_history,
            'last_fetch_timeout': self.last_fetch_timeout,
            'netloc_seen': self.netloc_seen,
            'netloc_rp': self.netloc_rp,
            'netloc_pause_until': self.netloc_pause_until,
            'netloc_consecutive_timeout_count': self.netloc_consecutive_timeout_count,
            'netloc_consecutive_timeout_pause_count': self.netloc_consecutive_timeout_pause_count,
            'netloc_consecutive_fetch_count': self.netloc_consecutive_fetch_count,
        }
        
        with open('crawler_state.pkl', 'wb') as f:
            pickle.dump(state, f)

        print(f"\nCrawler state saved to crawler_state.pkl")

    def load_state(self, pickle_file_path):
        """Load crawler state from pickle file"""
        with open(pickle_file_path, 'rb') as f:
            state = pickle.load(f)
        
        self.frontier_q = state['frontier_q']
        self.visited = state['visited']
        self.HTML_LIMIT = state['HTML_LIMIT']
        self.html_count = state['html_count']
        self.NETLOC_PAGE_LIMIT = state['NETLOC_PAGE_LIMIT']
        self.netloc_page_count = state['netloc_page_count']
        self.url_fetch_history = state['url_fetch_history']
        self.last_fetch_timeout = state['last_fetch_timeout']
        self.netloc_seen = state['netloc_seen']
        self.netloc_rp = state['netloc_rp']
        self.netloc_pause_until = state['netloc_pause_until']
        self.netloc_consecutive_timeout_count = state['netloc_consecutive_timeout_count']
        self.netloc_consecutive_timeout_pause_count = state['netloc_consecutive_timeout_pause_count']
        self.netloc_consecutive_fetch_count = state['netloc_consecutive_fetch_count']

        print(f"Loaded crawler state from {pickle_file_path}")

    def save_url_fetch_history(self, url):
        # -> most recent, ..., least recent
        # current_url is at index 0
        self.url_fetch_history.insert(0, url)
        if len(self.url_fetch_history) > self.NETLOC_CONSECUTIVE_TIMEOUT_PAUSE_TRIGGER:
            self.url_fetch_history.pop()

    def get_last_fetch_netloc(self):
        if len(self.url_fetch_history) < 2:
            return ""
        return urlsplit(self.url_fetch_history[1]).netloc

    def requeue_url_fetch_history(self):
        for url in self.url_fetch_history:
            self.frontier_q.insert(0, url)
            try:
                self.visited.remove(url)
            except KeyError:
                pass

    def get_raw_document(self, url, expected_content_type: ContentType):
        headers = {"User-Agent": self.USER_AGENT, "From": self.FROM_EMAIL}
        response = requests.get(url, headers=headers, timeout=5)
        response.raise_for_status()
        actual_content_type = response.headers["content-type"]
        if not expected_content_type.value in actual_content_type:
            raise ContentTypeException(expected_content_type.value, actual_content_type)
        if expected_content_type == ContentType.HTML:
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
                and self.netloc_page_count.get(url_parts.netloc, 0) < self.NETLOC_PAGE_LIMIT
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
        def signal_handler(signum, frame):
            print("\nReceived interrupt signal. Saving state before exit...")
            self.save_state()
            sys.exit(0)

        # Register the signal handler
        signal.signal(signal.SIGINT, signal_handler)

        print(f"Crawling {self.HTML_LIMIT} HTML pages, max {self.NETLOC_PAGE_LIMIT} pages per netloc")

        start_time = time.time()

        while len(self.frontier_q) > 0 and self.html_count < self.HTML_LIMIT:
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

        if self.get_last_fetch_netloc() == current_netloc:
            self.netloc_consecutive_fetch_count[current_netloc] += 1
        else:
            self.netloc_consecutive_fetch_count[current_netloc] = 1
            if self.get_last_fetch_netloc():
                self.netloc_consecutive_fetch_count[self.get_last_fetch_netloc()] = 0

        if self.netloc_consecutive_fetch_count[current_netloc] == self.NETLOC_CONSECUTIVE_FETCH_PAUSE_TRIGGER:
            self.netloc_pause_until[current_netloc] = time.time() + self.NETLOC_CONSECUTIVE_FETCH_PAUSE_SEC
            print(f"{current_netloc} has reached max consecutive fetch count. Pausing for {self.NETLOC_CONSECUTIVE_FETCH_PAUSE_SEC} seconds")

    def process_url(self, current_url):
        current_netloc = urlsplit(current_url).netloc

        if current_netloc not in self.netloc_page_count:
            self.netloc_page_count[current_netloc] = 0
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
            self.netloc_page_count[current_netloc] += 1
            if self.netloc_page_count[current_netloc] == self.NETLOC_PAGE_LIMIT:
                self.frontier_q = [url for url in self.frontier_q if urlsplit(url).netloc != current_netloc]
                print(f"Reached max pages for {current_netloc}. Removing from frontier")
            print(f"#{self.html_count} Got html from {current_url}")

            self.netloc_consecutive_timeout_count[current_netloc] = 0
            self.netloc_consecutive_timeout_count[self.get_last_fetch_netloc()] = 0
            self.netloc_consecutive_timeout_pause_count[current_netloc] = 0

            raw_urls_in_page = self.get_raw_urls_in_page(raw_html)
            normalized_urls = self.get_normalized_urls(current_url, raw_urls_in_page)
            old_frontier_q_size = len(self.frontier_q)
            self.filter_and_enqueue_urls(normalized_urls)
            print(f"    Found {len(self.frontier_q) - old_frontier_q_size} new urls ({len(self.frontier_q)} in frontier)")

        except (ConnectTimeout, ReadTimeout):
            current_fetch_timeout = True
            print(f"Timeout fetching {current_url}")

            # Handle netloc consecutive timeout
            if current_netloc == self.get_last_fetch_netloc():
                self.netloc_consecutive_timeout_count[current_netloc] += 1
            else:
                self.netloc_consecutive_timeout_count[current_netloc] = 1
                if self.get_last_fetch_netloc():
                    self.netloc_consecutive_timeout_count[self.get_last_fetch_netloc()] = 0
            
            if self.netloc_consecutive_timeout_count[current_netloc] == self.NETLOC_CONSECUTIVE_TIMEOUT_PAUSE_TRIGGER:
                self.netloc_consecutive_timeout_count[current_netloc] = 0
                self.netloc_consecutive_timeout_pause_count[current_netloc] += 1
                pause_duration = self.NETLOC_CONSECUTIVE_TIMEOUT_INITIAL_PAUSE_SEC * 2 ** (self.netloc_consecutive_timeout_pause_count[current_netloc] - 1)
                self.netloc_pause_until[current_netloc] = time.time() + pause_duration
                self.requeue_url_fetch_history()
                print(f"{current_netloc} has reached max consecutive timeout count. Pausing for {pause_duration} seconds")

        except Exception as e:
            print(f"Failed to process URL {current_url}: {e}")

        finally:
            self.last_fetch_timeout = current_fetch_timeout

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

    parser = argparse.ArgumentParser(description='Web crawler for ku.ac.th domains')
    
    parser.add_argument('-l', '--html-limit', 
                       type=int, 
                       required=False,
                       default=10000,
                       help='Maximum number of HTML pages to crawl')
    
    parser.add_argument('-n', '--netloc-limit',
                       type=int,
                       required=False,
                       default=100,
                       help='Maximum number of pages to crawl per netloc')
    
    parser.add_argument('-s', '--state-dir',
                       type=str,
                       required=False,
                       default=None,
                       help='Path to the pickle file containing the crawler state to resume from')

    args = parser.parse_args()

    crawler = WebCrawler(
        initial_urls=initial_urls,
        html_limit=args.html_limit,
        netloc_page_limit=args.netloc_limit,
        pickle_file_path=args.state_dir
    )

    crawler.crawl()


if __name__ == "__main__":
    main()
