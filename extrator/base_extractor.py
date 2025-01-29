from abc import ABC, abstractmethod

class BaseExtractor(ABC):
    
    @abstractmethod
    def read_csv(self):
        """Read data from csv."""
        pass