from scraping_utils import fetch_url
from queue_manager import task_queue
from config import BASE_URL
import logging
import aiohttp

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