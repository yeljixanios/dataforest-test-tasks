from scraper import get_product_links
from queue_manager import task_queue, db_queue, start_workers, stop_workers
from database import init_db, count_records
from config import CATEGORIES
import logging
import asyncio
import signal
import sys


logging.basicConfig(level=logging.INFO)

def signal_handler(signum, frame):
    print("\nGracefully shutting down...")
    sys.exit(0)

def main():
    """init db and start scrap process."""
    signal.signal(signal.SIGINT, signal_handler)
    init_db()

    async def run_scraping():
        for category in CATEGORIES:
            product_links = await get_product_links(category)
            logging.info(f"Found {len(product_links)} products in category {category}")

    threads = start_workers()
    
    try:
        before_count = count_records()
        logging.info(f"Records before scraping: {before_count}")

        asyncio.run(run_scraping())
        task_queue.join()
        db_queue.join()
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, shutting down...")
    finally:
        after_count = count_records()
        logging.info(f"Records after scraping: {after_count}")

        if after_count > before_count:
            logging.info("✅ All scraping results are stored in the database.")
        else:
            logging.warning("⚠ Some data might be missing in the database!")

        stop_workers(threads)

if __name__ == "__main__":
    main()