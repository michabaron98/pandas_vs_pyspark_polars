from extrator.base_extractor import BaseExtractor
from polars import DataFrame, read_csv

class PolarsExtractor(BaseExtractor):
    def __init__(self, file_name):
        super().__init__()
        self.file_name = file_name
    
    def read_csv(self) -> DataFrame:
        return read_csv(self.file_name)
    