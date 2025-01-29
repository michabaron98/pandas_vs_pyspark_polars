from extrator.base_extractor import BaseExtractor

from pyspark import spark

class PysparkTransformer(BaseExtractor):
    def __init__(self):
        super().__init__()
    
    def read_csv(self):
        print("Dropping duplicates using PySpark")
    