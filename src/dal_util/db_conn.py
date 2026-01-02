from abc import ABC, abstractmethod
import logging

logger = logging.getLogger(__name__)


class DatabaseConnectionInterface(ABC):
    @abstractmethod
    def get_connection(self):
        """Get a database connection."""
        pass

    @abstractmethod
    def close_connection(self):
        """Close the database connection."""
        pass