import threading
from queue import Queue, Empty
from scraping_utils import extract_product_info
from database import save_to_db
from config import THREAD_COUNT
import logging
import aiohttp
import asyncio
import time

task_queue = Queue()
db_queue = Queue()
shutdown_event = threading.Event()

def worker():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    session = aiohttp.ClientSession()
    try:
        while not shutdown_event.is_set():
            try:
                url, category = task_queue.get(timeout=1)
            except Empty:
                continue
            try:
                data = loop.run_until_complete(extract_product_info(session, url, category))
                if data:
                    db_queue.put(data)
                    logging.info(f"Processed and queued data for {url}")
            except Exception as e:
                logging.error(f"Failed to process {url}: {e}")
            finally:
                task_queue.task_done()
    finally:
        loop.run_until_complete(session.close())
        loop.close()

def db_writer():
    while not (shutdown_event.is_set() and db_queue.empty()):
        try:
            data = db_queue.get(timeout=1)
            save_to_db(data)
            db_queue.task_done()
        except Empty:
            continue

def start_workers():
    threads = []
    
    for _ in range(THREAD_COUNT):
        thread = threading.Thread(target=worker)
        thread.daemon = True
        thread.start()
        threads.append(thread)
    
    db_thread = threading.Thread(target=db_writer)
    db_thread.daemon = True
    db_thread.start()
    threads.append(db_thread)
    
    return threads

def stop_workers(threads):
    shutdown_event.set()
    while not task_queue.empty() or not db_queue.empty():
        time.sleep(0.5)
    for thread in threads:
        thread.join(timeout=2)