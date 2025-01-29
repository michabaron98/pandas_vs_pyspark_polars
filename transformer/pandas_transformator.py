from transformer.base_transformator import BaseTransformer


class PandasTransformer(BaseTransformer):
    def __init__(self, df):
        super().__init__(df)
    
    def drop_duplicates(self):
        print("Dropping duplicates using Pandas")
    
    def replace_nulls(self):
        print("Replacing nulls using Pandas")
    
    def replace_string(self):
        print("Replacing strings using Pandas")