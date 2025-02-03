from pipeline.pandas_pipeline import PandasPipeline

from pipeline.polars_pipeline import PolarsPipeline

from pipeline.pyspark_pipeline import PysparkPipeline

FILE_NAME = 'data/output_100.csv'
pandas_extractor = PandasPipeline(file_name=FILE_NAME)
data = pandas_extractor.run()


polars_extractor = PolarsPipeline(file_name=FILE_NAME)
data = polars_extractor.run()
print(data)