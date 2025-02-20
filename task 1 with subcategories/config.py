import os

BASE_URL = "https://www.vendr.com"
CATEGORIES = ["devops", "it-infrastructure", "data-analytics-and-management"]
DB_NAME = "products.db"
THREAD_COUNT = int(os.getenv("THREAD_COUNT", 5))  # Make thread count configurable