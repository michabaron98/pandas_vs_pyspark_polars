from pipeline.base_pipeline import BasePipeline
from common.timer import calculate_execution_time

import pandas as pd


class PandasPipeline(BasePipeline):
    def __init__(self, file_name):
        super().__init__()
        self.file_name = file_name
    
    def read_csv(self) -> pd.DataFrame:
        return pd.read_csv(self.file_name)
    
    def drop_duplicates(self, df: pd.DataFrame):
        return df.drop_duplicates()
    
    def replace_nulls(self, df: pd.DataFrame, value):
        return df.fillna(value=value)
    
    def replace_string(self, df: pd.DataFrame,column: str, pattern: str, new_value: str):
        df[column] = df[column].replace(to_replace=pattern, value=new_value, regex=True)
        return df

    def save_to_parquet(self, df:pd.DataFrame , 
                        file_name: str = 'data/output_pandas.parquet', 
                        compression: str='gzip', 
                        engine:str='pyarrow'):
        df.to_parquet(file_name,
                      engine=engine,
                        compression=compression)
        
    @calculate_execution_time
    def run(self):
        df_step_1 = self.read_csv()
        df_step_2 = self.drop_duplicates(df_step_1)
        df_step_3 = self.replace_string(df=df_step_2, column='Name', pattern=r"ae\b", new_value='AE')
        df_step_4 = self.replace_nulls(df_step_3, value='replaced_value')
        self.save_to_parquet(df_step_4)



