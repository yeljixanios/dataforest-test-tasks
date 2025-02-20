from scraper import get_product_links, get_subcategories, get_products_from_subcategory
from queue_manager import task_queue, db_queue, start_workers, stop_workers
from database import init_db
from config import CATEGORIES
import logging
import asyncio
import signal
import sys

logging.basicConfig(level=logging.INFO)

def signal_handler(signum, frame):
    print("\nGracefully shutting down...")

def main():
    """init db and start scrap process."""
    signal.signal(signal.SIGINT, signal_handler)
    init_db()

    async def run_scraping():
        for category in CATEGORIES:
            # First get products from main category
            product_links = await get_product_links(category)
            logging.info(f"Found {len(product_links)} products in category {category}")

            # Then we get subcategory
            subcategories = await get_subcategories(category)
            if not subcategories:
                logging.warning(f"No subcategories found for category {category}.")
            for subcategory in subcategories:
                await get_products_from_subcategory(subcategory, category)
                logging.info(f"Processed products from subcategory {subcategory}")

    threads = start_workers()
    
    try:
        asyncio.run(run_scraping())
        task_queue.join()
        db_queue.join()
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, shutting down...")
    finally:
        stop_workers(threads)

if __name__ == "__main__":
    main()