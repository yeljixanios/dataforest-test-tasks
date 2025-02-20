import sqlite3
from config import DB_NAME
import logging

def init_db():
    """Initialize database with products table"""
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                category TEXT,
                price_range TEXT,
                median_price TEXT,
                description TEXT
            );
        """)
        conn.commit()

def validate_data(data: dict) -> bool:
    """Validate product data before saving"""
    required_fields = ['name', 'category', 'description']
    
    # Check required fields
    if not all(data.get(field) for field in required_fields):
        logging.warning(f"Missing required fields in data: {data}")
        return False
    
    # Validate string lengths
    max_lengths = {
        'name': 255,
        'category': 100,
        'price_range': 50,
        'median_price': 50,
        'description': 1000
    }
    
    for field, max_length in max_lengths.items():
        if data.get(field) and len(str(data[field])) > max_length:
            logging.warning(f"Field {field} exceeds maximum length of {max_length}")
            return False
    
    return True

def save_to_db(data):
    """save to db, avoid duplicates"""
    if not validate_data(data):
        return False
    
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            sql = """
                INSERT OR IGNORE INTO products 
                (name, category, price_range, median_price, description) 
                VALUES (?, ?, ?, ?, ?)
            """
            values = (
                data.get("name"), 
                data.get("category"), 
                data.get("price_range"),  
                data.get("median_price"),
                data.get("description")
            )
            cursor.execute(sql, values)
            conn.commit()
            logging.info(f"Data saved to DB: {values}")
            return True
    except Exception as e:
        logging.error(f"Error saving to DB: {e}")
        return False