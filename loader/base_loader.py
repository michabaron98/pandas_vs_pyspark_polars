from abc import ABC, abstractmethod

class BaseLoader(ABC):
    
    @abstractmethod
    def save_to_parquet(self):
        """Read data from csv."""
        pass