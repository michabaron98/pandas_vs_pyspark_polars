from pipeline.pandas_pipeline import PandasPipeline
from pipeline.polars_pipeline import PolarsPipeline
from pipeline.pyspark_pipeline import PysparkPipeline
from data.generate_sample_data import RandomFileGenerator

from datetime import datetime


RECORDS = 1 * 10 ** 8
generator = RandomFileGenerator(columns={"Name": str, 
                                         "Surname": str, 
                                         "Department": int,
                                         "Age": int, 
                                         "Salary": float, 
                                         "Active": bool, 
                                         "Timestamp": datetime}, 
                                records=RECORDS)
generator.generate_csv()
FILE_NAME = f'data/output_{RECORDS}.csv'

pyspark_extractor = PysparkPipeline(file_name=FILE_NAME)
pyspark_extractor.run()
del pyspark_extractor

pandas_extractor = PandasPipeline(file_name=FILE_NAME)
pandas_extractor.run()
del pandas_extractor

polars_extractor = PolarsPipeline(file_name=FILE_NAME)
polars_extractor.run()
del polars_extractor