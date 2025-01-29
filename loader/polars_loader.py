from loader.base_loader import BaseLoader
from polars import DataFrame

class PolarsLoader(BaseLoader):
    
    def save_to_parquet(self, df:DataFrame , 
                        file_name: str = 'data/output_pandas.parquet', 
                        compression: str='gzip', 
                        engine:str='pyarrow'):
        use_pyarrow = True if engine=='pyarrow' else False
        df.write_parquet(file_name, compression=compression,
                         use_pyarrow=use_pyarrow)