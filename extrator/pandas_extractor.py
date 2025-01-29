from extrator.base_extractor import BaseExtractor
from pandas import DataFrame, read_csv

class PandasExtractor(BaseExtractor):
    def __init__(self, file_name):
        super().__init__()
        self.file_name = file_name
    
    def read_csv(self) -> DataFrame:
        return read_csv(self.file_name)