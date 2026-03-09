import csv
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.client import init_database, get_connection, execute_many, DatabaseError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CSV_PATH = Path(__file__).parent.parent / "cars_on_sale.csv"
BATCH_SIZE = 1000


def load_csv_data() -> int:
    """
    Load data from CSV file into the cars table.
    
    Returns:
        Number of rows inserted
        
    Raises:
        DatabaseError: If loading fails
    """
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")
    
    logger.info(f"Loading data from {CSV_PATH}")
    
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        total_rows = len(rows)
        logger.info(f"Found {total_rows} rows in CSV")
    
    insert_query = """
        INSERT INTO cars (
            date_of_manufacturing,
            engine_power,
            fuel_type,
            latitude_coordinates,
            longitude_coordinates,
            manufacturer,
            mileage,
            price,
            seller_country_code,
            seller_location
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    params_list = []
    for row in rows:
        try:
            params = (
                int(row["date_of_manufacturing"]) if row.get("date_of_manufacturing") else None,
                float(row["engine_power"]) if row.get("engine_power") else None,
                row.get("fuel_type"),
                float(row["latitude_coordinates"]) if row.get("latitude_coordinates") else None,
                float(row["longitude_coordinates"]) if row.get("longitude_coordinates") else None,
                row.get("manufacturer"),
                int(row["mileage"]) if row.get("mileage") else None,
                float(row["price"]) if row.get("price") else None,
                row.get("seller_country_code"),
                row.get("seller_location"),
            )
            params_list.append(params)
        except (ValueError, KeyError) as e:
            logger.warning(f"Skipping row due to parsing error: {e}")
            continue
    
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM cars")
        
        for i in range(0, len(params_list), BATCH_SIZE):
            batch = params_list[i:i + BATCH_SIZE]
            cursor.executemany(insert_query, batch)
            conn.commit()
            logger.info(f"Inserted {min(i + BATCH_SIZE, len(params_list))}/{len(params_list)} rows")
        
        return len(params_list)
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Failed to load CSV data: {e}")
        raise DatabaseError(f"Failed to load CSV data: {e}") from e
    finally:
        if conn:
            conn.close()


def main():
    """Initialize database and load CSV data."""
    try:
        logger.info("Initializing database schema...")
        init_database()
        
        logger.info("Loading CSV data...")
        rows_inserted = load_csv_data()
        
        logger.info(f"Database setup complete! Inserted {rows_inserted} rows.")
        
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM cars")
            count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(DISTINCT manufacturer) FROM cars")
            brands = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(DISTINCT seller_country_code) FROM cars")
            countries = cursor.fetchone()[0]
            
            logger.info(f"Database contains: {count} cars, {brands} brands, {countries} countries")
            
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
