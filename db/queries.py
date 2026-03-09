from typing import Optional
from .client import execute_query, execute_write, QueryError


def get_cars_by_brand(
    brand: str,
    limit: Optional[int] = None,
    offset: int = 0
) -> list[dict]:
    """
    Get cars for a specific manufacturer/brand with pagination.
    
    Args:
        brand: Manufacturer name (case-insensitive)
        limit: Maximum number of results (None for no limit)
        offset: Number of rows to skip
        
    Returns:
        List of car records as dictionaries
        
    Raises:
        QueryError: If query fails
    """
    query = """
        SELECT 
            date_of_manufacturing,
            engine_power,
            fuel_type,
            latitude_coordinates,
            longitude_coordinates,
            manufacturer,
            mileage,
            price,
            seller_country_code
        FROM cars 
        WHERE LOWER(manufacturer) = LOWER(?)
        ORDER BY price DESC
        LIMIT ? OFFSET ?
    """
    params = (brand, limit if limit else -1, offset)
    return execute_query(query, params) or []


def get_cars_by_brands(
    brands: list[str],
    limit: Optional[int] = None,
    offset: int = 0
) -> tuple[list[dict], list[str]]:
    """
    Get cars for multiple manufacturers with pagination.
    
    Args:
        brands: List of manufacturer names
        limit: Maximum number of results
        offset: Number of rows to skip
        
    Returns:
        Tuple of (car records list, unique country codes list)
        
    Raises:
        QueryError: If query fails
    """
    placeholders = ",".join(["LOWER(?)"] * len(brands))
    query = f"""
        SELECT 
            date_of_manufacturing,
            engine_power,
            fuel_type,
            latitude_coordinates,
            longitude_coordinates,
            manufacturer,
            mileage,
            price,
            seller_country_code
        FROM cars 
        WHERE LOWER(manufacturer) IN ({placeholders})
        ORDER BY manufacturer, price DESC
        LIMIT ? OFFSET ?
    """
    params = tuple(brands) + (limit if limit else -1, offset)
    cars = execute_query(query, params) or []
    
    country_codes = list(set(
        car["seller_country_code"] 
        for car in cars 
        if car.get("seller_country_code")
    ))
    
    return cars, country_codes


def get_all_brands(limit: Optional[int] = None, offset: int = 0) -> list[str]:
    """
    Get unique manufacturer names with pagination.
    
    Args:
        limit: Maximum number of results
        offset: Number of rows to skip
        
    Returns:
        List of manufacturer names
        
    Raises:
        QueryError: If query fails
    """
    query = """
        SELECT DISTINCT manufacturer FROM cars 
        ORDER BY manufacturer
        LIMIT ? OFFSET ?
    """
    result = execute_query(query, (limit if limit else -1, offset)) or []
    return [row["manufacturer"] for row in result]


def get_cars_by_country(
    country_code: str,
    limit: Optional[int] = None,
    offset: int = 0
) -> list[dict]:
    """
    Get cars from a specific country with pagination.
    
    Args:
        country_code: Country code (e.g., 'AT', 'DE')
        limit: Maximum number of results
        offset: Number of rows to skip
        
    Returns:
        List of car records
        
    Raises:
        QueryError: If query fails
    """
    query = """
        SELECT 
            date_of_manufacturing,
            engine_power,
            fuel_type,
            latitude_coordinates,
            longitude_coordinates,
            manufacturer,
            mileage,
            price,
            seller_country_code
        FROM cars 
        WHERE seller_country_code = ?
        ORDER BY price DESC
        LIMIT ? OFFSET ?
    """
    params = (country_code, limit if limit else -1, offset)
    return execute_query(query, params) or []


def get_distinct_countries(limit: Optional[int] = None, offset: int = 0) -> list[str]:
    """
    Get unique country codes with pagination.
    
    Args:
        limit: Maximum number of results
        offset: Number of rows to skip
        
    Returns:
        List of country codes
        
    Raises:
        QueryError: If query fails
    """
    query = """
        SELECT DISTINCT seller_country_code FROM cars 
        ORDER BY seller_country_code
        LIMIT ? OFFSET ?
    """
    result = execute_query(query, (limit if limit else -1, offset)) or []
    return [row["seller_country_code"] for row in result if row["seller_country_code"]]


def get_car_count(filter_brand: Optional[str] = None, filter_country: Optional[str] = None) -> int:
    """
    Get total number of cars with optional filters.
    
    Args:
        filter_brand: Filter by brand name
        filter_country: Filter by country code
        
    Returns:
        Total count
        
    Raises:
        QueryError: If query fails
    """
    query = "SELECT COUNT(*) as count FROM cars"
    params = []
    
    conditions = []
    if filter_brand:
        conditions.append("LOWER(manufacturer) = LOWER(?)")
        params.append(filter_brand)
    if filter_country:
        conditions.append("seller_country_code = ?")
        params.append(filter_country)
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    result = execute_query(query, tuple(params) if params else None, fetch_one=True)
    return result["count"] if result else 0


def get_car_count_by_brand(brand: str) -> int:
    """
    Get count of cars for a specific brand.
    
    Args:
        brand: Manufacturer name
        
    Returns:
        Number of cars
        
    Raises:
        QueryError: If query fails
    """
    query = "SELECT COUNT(*) as count FROM cars WHERE LOWER(manufacturer) = LOWER(?)"
    result = execute_query(query, (brand,), fetch_one=True)
    return result["count"] if result else 0


def get_cars_by_price_range(
    min_price: float,
    max_price: float,
    limit: Optional[int] = None,
    offset: int = 0
) -> list[dict]:
    """
    Get cars within a price range with pagination.
    
    Args:
        min_price: Minimum price (inclusive)
        max_price: Maximum price (inclusive)
        limit: Maximum number of results
        offset: Number of rows to skip
        
    Returns:
        List of car records
        
    Raises:
        QueryError: If query fails
    """
    query = """
        SELECT 
            date_of_manufacturing,
            engine_power,
            fuel_type,
            latitude_coordinates,
            longitude_coordinates,
            manufacturer,
            mileage,
            price,
            seller_country_code
        FROM cars 
        WHERE price BETWEEN ? AND ?
        ORDER BY price DESC
        LIMIT ? OFFSET ?
    """
    params = (min_price, max_price, limit if limit else -1, offset)
    return execute_query(query, params) or []


def get_cars_by_fuel_type(
    fuel_type: str,
    limit: Optional[int] = None,
    offset: int = 0
) -> list[dict]:
    """
    Get cars by fuel type with pagination.
    
    Args:
        fuel_type: Fuel type (e.g., 'diesel', 'electric', 'hybrid')
        limit: Maximum number of results
        offset: Number of rows to skip
        
    Returns:
        List of car records
        
    Raises:
        QueryError: If query fails
    """
    query = """
        SELECT 
            date_of_manufacturing,
            engine_power,
            fuel_type,
            latitude_coordinates,
            longitude_coordinates,
            manufacturer,
            mileage,
            price,
            seller_country_code
        FROM cars 
        WHERE LOWER(fuel_type) = LOWER(?)
        ORDER BY price DESC
        LIMIT ? OFFSET ?
    """
    params = (fuel_type, limit if limit else -1, offset)
    return execute_query(query, params) or []


def get_all_cars(
    limit: Optional[int] = None,
    offset: int = 0,
    order_by: str = "price",
    order_dir: str = "DESC"
) -> list[dict]:
    """
    Get all cars with pagination and sorting.
    
    Args:
        limit: Maximum number of results
        offset: Number of rows to skip
        order_by: Column to sort by (default: price)
        order_dir: Sort direction (ASC or DESC)
        
    Returns:
        List of car records
        
    Raises:
        QueryError: If query fails
    """
    allowed_columns = {
        "date_of_manufacturing", "engine_power", "fuel_type",
        "latitude_coordinates", "longitude_coordinates", "manufacturer",
        "mileage", "price", "seller_country_code"
    }
    allowed_directions = {"ASC", "DESC"}
    
    if order_by not in allowed_columns:
        order_by = "price"
    if order_dir not in allowed_directions:
        order_dir = "DESC"
    
    query = f"""
        SELECT 
            date_of_manufacturing,
            engine_power,
            fuel_type,
            latitude_coordinates,
            longitude_coordinates,
            manufacturer,
            mileage,
            price,
            seller_country_code
        FROM cars 
        ORDER BY {order_by} {order_dir}
        LIMIT ? OFFSET ?
    """
    params = (limit if limit else -1, offset)
    return execute_query(query, params) or []


def bulk_insert_cars(cars_data: list[dict]) -> int:
    """
    Bulk insert multiple car records.
    
    Args:
        cars_data: List of dictionaries with car data
        
    Returns:
        Number of rows inserted
        
    Raises:
        QueryError: If insert fails
    """
    from .client import execute_many
    
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
    for row in cars_data:
        params = (
            row.get("date_of_manufacturing"),
            row.get("engine_power"),
            row.get("fuel_type"),
            row.get("latitude_coordinates"),
            row.get("longitude_coordinates"),
            row.get("manufacturer"),
            row.get("mileage"),
            row.get("price"),
            row.get("seller_country_code"),
            row.get("seller_location"),
        )
        params_list.append(params)
    
    return execute_many(insert_query, params_list)


def bulk_update_prices(updates: list[tuple]) -> int:
    """
    Bulk update prices for multiple cars.
    
    Args:
        updates: List of tuples (new_price, car_id)
        
    Returns:
        Number of rows updated
        
    Raises:
        QueryError: If update fails
    """
    query = "UPDATE cars SET price = ? WHERE id = ?"
    from .client import execute_many
    return execute_many(query, updates)


def bulk_delete_cars(car_ids: list[int]) -> int:
    """
    Bulk delete cars by IDs.
    
    Args:
        car_ids: List of car IDs to delete
        
    Returns:
        Number of rows deleted
        
    Raises:
        QueryError: If delete fails
    """
    if not car_ids:
        return 0
    
    placeholders = ",".join(["?"] * len(car_ids))
    query = f"DELETE FROM cars WHERE id IN ({placeholders})"
    return execute_write(query, tuple(car_ids))
