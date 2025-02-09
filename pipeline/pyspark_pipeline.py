import os
from datetime import datetime

from pipeline.base_pipeline import BasePipeline
from common.timer import calculate_execution_time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace


class PysparkPipeline(BasePipeline):
    
    def __init__(self, file_name):
        super().__init__()
        self.file_name = file_name
        os.environ['HADOOP_USER_NAME'] = 'root'
        os.environ['SPARK_USER'] = 'root'
        
        # Get JAVA_HOME dynamically in Python (instead of using shell syntax)
        java_home = os.popen("/usr/libexec/java_home -v 17").read().strip()
        os.environ['JAVA_HOME'] = java_home
        self.session = SparkSession.builder.appName("PysparkPipeline")\
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")\
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")\
                    .config("spark.driver.host", "127.0.0.1")\
                        .config("spark.ui.showConsoleProgress", "false")\
                            .getOrCreate()
        self.output_file_name = f"data/output/output_pyspark_{datetime.now()}.parquet"

    def read_csv(self):
        return self.session.read.csv(self.file_name, header=True, inferSchema=True)
    
    def drop_duplicates(self, df):
        return df.dropDuplicates()
    
    def replace_nulls(self, df, value):
        return df.fillna(value)
    
    def replace_string(self, df, column: str, pattern: str, new_value: str):
        return df.withColumn(column, regexp_replace(col(column), pattern, new_value))
    
    def save_to_parquet(self, df, 
                        compression: str = 'gzip'):
        df.write.parquet(self.output_file_name, compression=compression)
        
    @calculate_execution_time
    def run(self):
        df_step_1 = self.read_csv()
        df_step_2 = self.drop_duplicates(df_step_1)
        df_step_3 = self.replace_string(df=df_step_2, column='Name', pattern=r"ae\\b", new_value='AE')
        df_step_4 = self.replace_nulls(df=df_step_3, value='replaced_value')
        self.save_to_parquet(df_step_4)



