from typing import Optional
from .async_client import execute_query as async_exec_query, AsyncQueryError


async def get_cars_by_brand(
    brand: str,
    limit: Optional[int] = None,
    offset: int = 0
) -> list[dict]:
    """Get cars for a specific brand with pagination."""
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
    result = await async_exec_query(query, params)
    return result if result else []


async def get_cars_by_brands(
    brands: list[str],
    limit: Optional[int] = None,
    offset: int = 0
) -> tuple[list[dict], list[str]]:
    """Get cars for multiple brands with pagination."""
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
    cars = await async_exec_query(query, params) or []
    
    country_codes = list(set(
        car["seller_country_code"]
        for car in cars
        if car.get("seller_country_code")
    ))
    
    return cars, country_codes


async def get_all_brands(limit: Optional[int] = None, offset: int = 0) -> list[str]:
    """Get all unique brands with pagination."""
    query = """
        SELECT DISTINCT manufacturer FROM cars 
        ORDER BY manufacturer
        LIMIT ? OFFSET ?
    """
    result = await async_exec_query(query, (limit if limit else -1, offset)) or []
    return [row["manufacturer"] for row in result]


async def get_all_cars(
    limit: Optional[int] = None,
    offset: int = 0,
    order_by: str = "price",
    order_dir: str = "DESC"
) -> list[dict]:
    """Get all cars with pagination and sorting."""
    allowed_columns = {
        "date_of_manufacturing", "engine_power", "fuel_type",
        "latitude_coordinates", "longitude_coordinates", "manufacturer",
        "mileage", "price", "seller_country_code"
    }
    
    if order_by not in allowed_columns:
        order_by = "price"
    order_dir = order_dir if order_dir in {"ASC", "DESC"} else "DESC"
    
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
    result = await async_exec_query(query, params)
    return result if result else []


async def get_car_count(
    filter_brand: Optional[str] = None,
    filter_country: Optional[str] = None
) -> int:
    """Get car count with optional filters."""
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
    
    result = await async_exec_query(query, tuple(params) if params else None, fetch_one=True)
    return result["count"] if result else 0
