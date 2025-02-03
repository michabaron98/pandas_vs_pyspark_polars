from abc import ABC, abstractmethod

class BasePipeline(ABC):
    
    def __init__(self):
        ...
    
    def read_csv(self):
        ...
    
    def drop_duplicates(self):
        ...
    
    def replace_nulls(self):
        ...
    
    def replace_string(self):
        ...

    def save_to_parquet(self):
        ...
        
    def run(self):
        ...
