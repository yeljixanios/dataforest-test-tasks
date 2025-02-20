from scraping_utils import fetch_url, extract_product_info 
from queue_manager import task_queue, db_queue
from config import BASE_URL
import logging
import aiohttp

async def get_subcategories(category: str):
    url = f"{BASE_URL}/categories/{category}"
    
    async with aiohttp.ClientSession() as session:
        try:
            tree = await fetch_url(session, url)
            subcategory_links = tree.xpath('//a[contains(@class, "rt-Link") and contains(span/text(), "View more")]/@href')
            subcategories = [f"{BASE_URL}{link}" for link in subcategory_links]
            logging.info(f"Found {len(subcategories)} subcategories for {category}: {subcategories}")
            return subcategories
        except Exception as e:
            logging.error(f"Failed to load subcategories for {category}: {e}")
            return []

async def get_product_links(category: str):
    url = f"{BASE_URL}/categories/{category}"

    async with aiohttp.ClientSession() as session:
        try:
            tree = await fetch_url(session, url)
            product_links = tree.xpath('//a[contains(@href, "/marketplace/")]/@href')
            product_links = [f"{BASE_URL}{link}" if link.startswith("/") else link for link in product_links]

            for link in product_links:
                task_queue.put((link, category))

            return product_links
        except Exception as e:
            logging.error(f"Failed to load page in get_product_links: {url} - {e}")
            return []

async def get_products_from_subcategory(subcategory: str, category: str):
    page = 1
    while True:
        # Kostil, tuck
        trimmed_subcategory = subcategory[:-1]  
        url = f"{trimmed_subcategory}{page}"  

        logging.info(f"Fetching URL: {url}")
        async with aiohttp.ClientSession() as session:
            try:
                tree = await fetch_url(session, url)
                product_links = tree.xpath('//a[contains(@href, "/marketplace/")]/@href')
                
                # If no products on current page, goto next subcategory
                if not product_links:
                    logging.info(f"No products found on page {page} for subcategory {url}. Moving to next subcategory.")
                    break  # Leave cycle, move on to next subcategory

                product_links = [f"{BASE_URL}{link}" if link.startswith("/") else link for link in product_links]
                for link in product_links:
                    data = await extract_product_info(session, link, category)  
                    if data:
                        db_queue.put(data)  
                        logging.info(f"Added product data to DB queue: {data}")

                logging.info(f"Found {len(product_links)} products on page {page} for subcategory {url}.")
                page += 1 
            except Exception as e:
                logging.error(f"Failed to load page in get_products_from_subcategory: {url} - {e}")
                break