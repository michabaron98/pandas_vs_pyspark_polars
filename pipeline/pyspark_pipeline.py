from pipeline.base_pipeline import BasePipeline

from pyspark import *

class PysparkPipeline(BasePipeline):
    
    def __init__(self, file_name):
        super().__init__()
        self.file_name = file_name
    
    def read_csv(self):
        print("Extracting Data")
    
    def drop_duplicates(self):
        print("Dropping duplicates using Pandas")
    
    def replace_nulls(self):
        print("Replacing nulls using Pandas")
    
    def replace_string(self):
        print("Replacing strings using Pandas")
    
    def save_to_parquet(self, df):
        df.write.parquet('output.parquet')
        
    def run(self):
        ...



