import aiosqlite
import logging
from typing import Optional

from .config import DB_PATH, DB_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AsyncDatabaseError(Exception):
    """Base exception for async database errors."""
    pass


class AsyncQueryError(AsyncDatabaseError):
    """Raised when an async query fails."""
    def __init__(self, message: str, query: Optional[str] = None):
        super().__init__(message)
        self.query = query


async def get_connection() -> aiosqlite.Connection:
    """
    Get an async SQLite connection.
    
    Returns:
        aiosqlite.Connection: Async database connection
        
    Raises:
        AsyncDatabaseError: If connection cannot be established
    """
    try:
        conn = await aiosqlite.connect(
            DB_PATH,
            timeout=DB_CONFIG.get("timeout", 30.0)
        )
        conn.row_factory = aiosqlite.Row
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise AsyncDatabaseError(f"Database connection failed: {e}") from e


async def execute_query(
    query: str,
    params: Optional[tuple] = None,
    fetch_one: bool = False
) -> Optional[dict]:
    """
    Execute an async SELECT query and return results as dictionaries.
    
    Args:
        query: SQL SELECT query
        params: Query parameters
        fetch_one: If True, return single row
        
    Returns:
        List of dictionaries or single dict
        
    Raises:
        AsyncQueryError: If query fails
    """
    try:
        async with await get_connection() as conn:
            cursor = await conn.execute(query, params or ())
            
            if fetch_one:
                row = await cursor.fetchone()
                return dict(row) if row else None
            else:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Async query failed: {e} | Query: {query}")
        raise AsyncQueryError(f"Query execution failed: {e}", query) from e


async def execute_write(
    query: str,
    params: Optional[tuple] = None,
    commit: bool = True
) -> int:
    """
    Execute an async INSERT, UPDATE, or DELETE query.
    
    Args:
        query: SQL write query
        params: Query parameters
        commit: Whether to commit
        
    Returns:
        Number of rows affected
        
    Raises:
        AsyncQueryError: If query fails
    """
    try:
        async with await get_connection() as conn:
            cursor = await conn.execute(query, params or ())
            
            if commit:
                await conn.commit()
            
            return cursor.rowcount
    except Exception as e:
        logger.error(f"Async write failed: {e} | Query: {query}")
        raise AsyncQueryError(f"Write operation failed: {e}", query) from e


async def execute_many(
    query: str,
    params_list: list[tuple],
    commit: bool = True
) -> int:
    """
    Execute a query multiple times with different parameters.
    
    Args:
        query: SQL query
        params_list: List of parameter tuples
        commit: Whether to commit
        
    Returns:
        Total rows affected
        
    Raises:
        AsyncQueryError: If query fails
    """
    try:
        async with await get_connection() as conn:
            cursor = await conn.cursor()
            await cursor.executemany(query, params_list)
            
            if commit:
                await conn.commit()
            
            return cursor.rowcount
    except Exception as e:
        logger.error(f"Async bulk write failed: {e} | Query: {query}")
        raise AsyncQueryError(f"Bulk write operation failed: {e}", query) from e


async def init_database() -> None:
    """Initialize the database with table schema."""
    schema = """
    CREATE TABLE IF NOT EXISTS cars (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date_of_manufacturing INTEGER,
        engine_power REAL,
        fuel_type TEXT,
        latitude_coordinates REAL,
        longitude_coordinates REAL,
        manufacturer TEXT NOT NULL,
        mileage INTEGER,
        price REAL,
        seller_country_code TEXT,
        seller_location TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_manufacturer ON cars(manufacturer);
    CREATE INDEX IF NOT EXISTS idx_country_code ON cars(seller_country_code);
    CREATE INDEX IF NOT EXISTS idx_price ON cars(price);
    """
    
    try:
        async with await get_connection() as conn:
            await conn.executescript(schema)
            await conn.commit()
            logger.info("Async database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise AsyncDatabaseError(f"Database initialization failed: {e}") from e
