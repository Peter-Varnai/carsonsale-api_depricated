import sqlite3
import logging
from contextlib import contextmanager
from typing import Any, Generator, Optional

from .config import DB_PATH, DB_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Base exception for database errors."""
    pass


class ConnectionError(DatabaseError):
    """Raised when database connection fails."""
    pass


class QueryError(DatabaseError):
    """Raised when a query fails."""
    def __init__(self, message: str, query: Optional[str] = None):
        super().__init__(message)
        self.query = query


def get_connection() -> sqlite3.Connection:
    """
    Get a SQLite connection with row factory set to return dictionaries.
    
    Returns:
        sqlite3.Connection: Database connection
        
    Raises:
        ConnectionError: If connection cannot be established
    """
    try:
        conn = sqlite3.connect(
            DB_PATH,
            timeout=DB_CONFIG.get("timeout", 30.0),
            check_same_thread=DB_CONFIG.get("check_same_thread", False)
        )
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        raise ConnectionError(f"Database connection failed: {e}") from e


@contextmanager
def get_connection_context() -> Generator[sqlite3.Connection, None, None]:
    """
    Context manager for database connections.
    
    Yields:
        sqlite3.Connection: Database connection
        
    Raises:
        ConnectionError: If connection cannot be established
    """
    conn = None
    try:
        conn = get_connection()
        yield conn
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise ConnectionError(f"Database error: {e}") from e
    finally:
        if conn:
            conn.close()


def execute_query(
    query: str, 
    params: Optional[tuple] = None,
    fetch_one: bool = False
) -> Optional[dict]:
    """
    Execute a SELECT query and return results as dictionaries.
    
    Args:
        query: SQL SELECT query
        params: Query parameters (for parameterized queries)
        fetch_one: If True, return single row instead of all rows
        
    Returns:
        List of dictionaries (or single dict if fetch_one=True)
        None if no results
        
    Raises:
        QueryError: If query execution fails
    """
    try:
        with get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            
            if fetch_one:
                row = cursor.fetchone()
                return dict(row) if row else None
            else:
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
    except sqlite3.Error as e:
        logger.error(f"Query failed: {e} | Query: {query}")
        raise QueryError(f"Query execution failed: {e}", query) from e


def execute_write(
    query: str, 
    params: Optional[tuple] = None,
    commit: bool = True
) -> int:
    """
    Execute an INSERT, UPDATE, or DELETE query.
    
    Args:
        query: SQL write query
        params: Query parameters
        commit: Whether to commit the transaction
        
    Returns:
        Number of rows affected
        
    Raises:
        QueryError: If query execution fails
    """
    try:
        with get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            
            if commit:
                conn.commit()
            
            return cursor.rowcount
    except sqlite3.Error as e:
        logger.error(f"Write failed: {e} | Query: {query}")
        raise QueryError(f"Write operation failed: {e}", query) from e


def execute_many(
    query: str, 
    params_list: list[tuple],
    commit: bool = True
) -> int:
    """
    Execute a query multiple times with different parameters.
    
    Args:
        query: SQL query to execute
        params_list: List of parameter tuples
        commit: Whether to commit the transaction
        
    Returns:
        Total number of rows affected
        
    Raises:
        QueryError: If query execution fails
    """
    try:
        with get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            
            if commit:
                conn.commit()
            
            return cursor.rowcount
    except sqlite3.Error as e:
        logger.error(f"Bulk write failed: {e} | Query: {query}")
        raise QueryError(f"Bulk write operation failed: {e}", query) from e


def init_database() -> None:
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
        with get_connection_context() as conn:
            conn.executescript(schema)
            conn.commit()
            logger.info("Database initialized successfully")
    except sqlite3.Error as e:
        logger.error(f"Failed to initialize database: {e}")
        raise DatabaseError(f"Database initialization failed: {e}") from e
