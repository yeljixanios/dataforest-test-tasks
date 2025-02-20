import multiprocessing
import sqlite3
import time
import traceback
import logging
import os
import psutil
from queue import Empty
from playwright.sync_api import sync_playwright
from typing import Dict, List, Any, Optional
from urllib.parse import urljoin

# config constants 
BASE_URL = "https://books.toscrape.com"
CATALOGUE_URL = f"{BASE_URL}/catalogue/page-{{0}}.html"
DB_NAME = "books_data.sqlite"
DEFAULT_PROCESSES = 3
DEFAULT_TIMEOUT = 30000  # ms
QUEUE_TIMEOUT = 10  # seconds
PROCESS_CHECK_INTERVAL = 5  # seconds
DEFAULT_MEMORY_LIMIT = 500  # MB
CDP_ENDPOINT = "http://localhost:9222"
NUM_PAGES = 50

# log set
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
LOG_FILE = "scraper.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format=LOG_FORMAT)

def log_message(message: str) -> None:
    print(message)
    logging.info(message)

def init_db() -> None:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            price TEXT,
            availability TEXT,
            rating TEXT,
            image_url TEXT,
            description TEXT,
            category TEXT,
            product_info TEXT
        )''')
        conn.commit()

def save_to_db(book_data: Dict[str, Any]) -> None:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('''INSERT INTO books (title, price, availability, rating, image_url, description, category, product_info)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                       (book_data["title"], book_data["price"], book_data["availability"],
                        book_data["rating"], book_data["image_url"], book_data["description"],
                        book_data["category"], str(book_data["product_info"])))
        conn.commit()

class BookScraper(multiprocessing.Process):
    def __init__(self, task_queue: multiprocessing.Queue, use_cdp: bool = False, 
                 memory_limit_mb: int = DEFAULT_MEMORY_LIMIT) -> None:
        super().__init__()
        self.task_queue = task_queue
        self.use_cdp = use_cdp
        self.memory_limit_mb = memory_limit_mb

    def check_resources(self) -> bool:
        """Check use of resources by the process"""
        process = psutil.Process(self.pid)
        memory_mb = process.memory_info().rss / 1024 / 1024
        return memory_mb < self.memory_limit_mb

    def run(self) -> None:
        log_message(f"[PID {self.pid}] Starting process")
        try:
            with sync_playwright() as p:
                browser = self._initialize_browser(p)
                page = browser.new_page()
                
                while True:
                    if not self.check_resources():
                        log_message(f"[PID {self.pid}] Memory limit exceeded, stopping process")
                        break
                        
                    try:
                        url = self.task_queue.get(timeout=QUEUE_TIMEOUT)
                    except Empty:
                        log_message(f"[PID {self.pid}] Queue is empty, finishing work")
                        break

                    try:
                        book_data = self.scrape_book(page, url)
                        log_message(f"[PID {self.pid}] Successfully processed book: {book_data['title']}")
                        save_to_db(book_data)
                    except Exception:
                        log_message(f"[PID {self.pid}] Error processing {url}: {traceback.format_exc()}")

        except Exception as e:
            log_message(f"[PID {self.pid}] Critical error: {str(e)}")
        finally:
            log_message(f"[PID {self.pid}] Process completed")

    def _initialize_browser(self, playwright):
        """Init browser with CDP support"""
        try:
            if self.use_cdp:
                browser = playwright.chromium.connect(CDP_ENDPOINT)
                log_message(f"[PID {self.pid}] CDP connection successful")
                return browser
        except Exception:
            log_message(f"[PID {self.pid}] CDP unavailable, starting local browser")
        
        return playwright.chromium.launch(headless=True)

    def scrape_book(self, page, url: str) -> Dict[str, Any]:
        page.goto(url, timeout=DEFAULT_TIMEOUT)

        title = page.locator(".product_main h1").inner_text()
        category = page.locator(".breadcrumb li:nth-child(3) a").inner_text()
        price = page.locator(".product_main .price_color").inner_text()
        
        rating_class = page.locator(".product_main .star-rating").get_attribute("class")
        rating = rating_class.split()[-1] if rating_class else "No rating"
        
        availability = page.locator(".product_main .instock.availability").inner_text().strip()
        
        image_url = page.locator(".item.active img").get_attribute("src")
        image_url = urljoin(BASE_URL, image_url) if image_url else "No image"
        
        description_locator = page.locator("#product_description + p")
        description = description_locator.inner_text() if description_locator.count() > 0 else "No description"
        
        product_info = {}
        for row in page.locator(".table-striped tr").all():
            key = row.locator("th").inner_text()
            value = row.locator("td").inner_text()
            product_info[key] = value
        
        return {
            "title": title,
            "price": price,
            "availability": availability,
            "rating": rating,
            "image_url": image_url,
            "description": description,
            "category": category,
            "product_info": product_info,
        }

class ProcessManager:
    def __init__(self, num_processes: int = DEFAULT_PROCESSES) -> None:
        self.num_processes = num_processes
        self.task_queue = multiprocessing.Queue()
        self.processes: List[multiprocessing.Process] = []

    def add_tasks(self, urls: List[str]) -> None:
        for url in urls:
            self.task_queue.put(url)

    def start_scraping(self) -> None:
        for _ in range(self.num_processes):
            self._start_new_process()
        
        while not self.task_queue.empty():
            self._monitor_processes()
            time.sleep(PROCESS_CHECK_INTERVAL)
        
        self._wait_for_completion()

    def _monitor_processes(self) -> None:
        """Monitoring and restarting fallen processes"""
        for i, p in enumerate(self.processes):
            if not p.is_alive():
                log_message(f"⚠️ [PID {p.pid}] Process failed, restarting...")
                self.processes[i] = self._start_new_process()

    def _wait_for_completion(self) -> None:
        """Waiting completion of all processes"""
        for p in self.processes:
            p.join()
        log_message("All processes completed")

    def _start_new_process(self) -> multiprocessing.Process:
        p = BookScraper(self.task_queue, use_cdp=False)
        p.start()
        self.processes.append(p)
        return p

def collect_book_urls() -> List[str]:
    """Collect URL of all books"""
    book_urls = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        for page_num in range(1, NUM_PAGES + 1):
            url = CATALOGUE_URL.format(page_num)
            page.goto(url, timeout=DEFAULT_TIMEOUT)
            links = page.locator("h3 a").evaluate_all("elements => elements.map(e => e.getAttribute('href'))")
            book_urls.extend([urljoin(BASE_URL, f"catalogue/{link}") for link in links])
            
        log_message(f"Collected {len(book_urls)} book URLs")
    return book_urls

if __name__ == "__main__":
    try:
        init_db()
        book_urls = collect_book_urls()
        
        manager = ProcessManager(num_processes=DEFAULT_PROCESSES)
        manager.add_tasks(book_urls)
        manager.start_scraping()
    except Exception:
        log_message(f"Critical error: {traceback.format_exc()}")