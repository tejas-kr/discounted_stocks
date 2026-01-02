import sqlite3
import threading
import logging

from .db_conn import DatabaseConnectionInterface


logger = logging.getLogger(__name__)


class SQLiteDatabaseConnection(DatabaseConnectionInterface):
    _instance = None
    _connection = None
    _lock = threading.Lock()

    def __new__(cls, db_path: str = ":memory:"):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(SQLiteDatabaseConnection, cls).__new__(cls)
                try:
                    cls._connection = sqlite3.connect(db_path, check_same_thread=False)
                    cls._connection.row_factory = sqlite3.Row  # Similar to RealDictCursor
                    logger.info("SQLite database connection initialized.")
                except Exception as e:
                    logger.error(f"Failed to initialize SQLite database connection: {e}")
                    raise
            return cls._instance

    @classmethod
    def get_connection(cls) -> sqlite3.Connection:
        if not cls._connection:
            raise RuntimeError("SQLite database connection not initialized. "
                               "Call SQLiteDatabaseConnection() with db_path first.")
        return cls._connection

    @classmethod
    def close_connection(cls) -> None:
        if cls._connection:
            cls._connection.close()
            cls._connection = None
            cls._instance = None
            logger.info("SQLite database connection closed.")
        else:
            logger.warning("No connection to close.")