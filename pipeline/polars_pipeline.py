from typing import Optional, Literal
from common.timer import calculate_execution_time

import polars as pl

from pipeline.base_pipeline import BasePipeline


class PolarsPipeline(BasePipeline):
    def __init__(self, file_name):
        super().__init__()
        self.file_name = file_name
    
    def read_csv(self) -> pl.DataFrame:
        return pl.read_csv(self.file_name)
    
    def drop_duplicates(self, df: pl.DataFrame):
        return df.unique(maintain_order=True)
    
    def replace_nulls(self, df: pl.DataFrame, value: Optional[str | int] = None, stragtegy: Literal[None, 'forward', 'backward', 'min', 'max', 'mean', 'zero', 'one'] = None):
        return df.fill_null(value, strategy=stragtegy)
    
    def replace_string(self, df: pl.DataFrame, column: str, pattern: str, new_value: str ):
        return df.with_columns(pl.col(column).str.replace( pattern, new_value))
    
    def save_to_parquet(self, df:pl.DataFrame , 
                        file_name: str = 'data/output_polars.parquet', 
                        compression: str='gzip', 
                        engine:str='pyarrow'):
        use_pyarrow = True if engine=='pyarrow' else False
        df.write_parquet(file_name, compression=compression,
                         use_pyarrow=use_pyarrow)
        
    @calculate_execution_time  
    def run(self):
        df_step_1 = self.read_csv()
        df_step_2 = self.drop_duplicates(df_step_1)
        df_step_3 = self.replace_string(df=df_step_2, column='Name', pattern=r"ae\b", new_value='AE')
        df_step_4 = self.replace_nulls(df_step_3, value='replaced_value')
        self.save_to_parquet(df_step_4)




