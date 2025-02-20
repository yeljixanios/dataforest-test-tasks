import aiohttp
from lxml import html
import logging

async def fetch_url(session, url: str) -> html.HtmlElement:
    """fetch HTML content + return like lxml element"""
    async with session.get(url) as response:
        response.raise_for_status()
        content = await response.text()
        return html.fromstring(content)

async def extract_product_info(session, product_url: str, category: str) -> dict:
    """extract product information"""
    product_info = {
        "name": None,
        "category": category,
        "price_range": None,
        "median_price": None,
        "description": None
    }
    
    try:
        tree = await fetch_url(session, product_url)
        
        name = tree.xpath('//h1[contains(@class, "rt-Heading")]/text()')
        if name:
            product_info["name"] = name[0].strip()
        
        price_low = tree.xpath('//div[contains(@class, "_rangeSlider_")]//span[1]/text()')
        price_high = tree.xpath('//div[contains(@class, "_rangeSlider_")]//span[last()]/text()')
        
        if price_low and price_high:
            product_info["price_range"] = f"{price_low[0].strip()}-{price_high[0].strip()}"
        
        median = tree.xpath('//div[contains(@class, "rt-Flex rt-r-ai-end rt-r-gap-2")]/span[1]/text()')
        if median:
            product_info["median_price"] = median[0].strip()
        
        desc = tree.xpath('//p[contains(@class, "rt-Text")]/text()')
        if desc:
            product_info["description"] = desc[0].strip()

        return product_info

    except Exception as e:
        logging.error(f"Failed to parse {product_url}: {e}")
        return None