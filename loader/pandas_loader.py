from loader.base_loader import BaseLoader
from pandas import DataFrame

class PandasLoader(BaseLoader):
    
    def save_to_parquet(self, df:DataFrame , 
                        file_name: str = 'data/output_pandas.parquet', 
                        compression: str='gzip', 
                        engine:str='pyarrow'):
        df.to_parquet(file_name,
                      engine=engine,
                        compression=compression)