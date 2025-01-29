from abc import ABC, abstractmethod

class BaseTransformer(ABC):
    
    @abstractmethod
    def drop_duplicates(self):
        """Remove duplicate rows from the dataset."""
        pass
    
    @abstractmethod
    def replace_nulls(self):
        """Replace null values in the dataset."""
        pass
    
    @abstractmethod
    def replace_string(self):
        """Replace specific strings in the dataset."""
        pass