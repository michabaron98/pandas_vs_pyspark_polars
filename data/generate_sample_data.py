import csv
import random
import string
from typing import Dict
from datetime import datetime, timedelta

from loguru import logger

class RandomFileGenerator:
    def __init__(self, columns: Dict[str, type], records: int, filename: str = "data/output.csv"):
        self.columns = columns
        self.records = records
        self.filename = filename

    @staticmethod
    def random_string(length=10):
        return ''.join(random.choices(string.ascii_letters, k=length))

    @staticmethod
    def random_int() -> int:
        return random.randint(1, 100)

    @staticmethod
    def random_float() -> float:
        return round(random.uniform(1, 100), 2)

    @staticmethod
    def random_bool() -> bool:
        return random.choice([True, False])
    
    @staticmethod
    def random_datetime(start_date="2000-01-01", end_date="2030-12-31"):
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        random_date = start + timedelta(days=random.randint(0, (end - start).days))
        return random_date.strftime("%Y-%m-%d %H:%M:%S")

    def generate_csv(self):
        type_generators = {
            str: self.random_string,
            int: self.random_int,
            float: self.random_float,
            bool: self.random_bool,
            datetime: self.random_datetime
        }

        with open(self.filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(self.columns.keys())  # Write headers

            for _ in range(self.records):
                row = [type_generators[col_type]() for col_type in self.columns.values()]
                writer.writerow(row)

        logger.info(f"CSV file '{self.filename}' generated with {self.records} records.")


# # Example usage:
# generator = RandomFileGenerator(columns={"Name": str, 
#                                          "Surname": str, 
#                                          "Department": int,
#                                          "Age": int, 
#                                          "Salary": float, 
#                                          "Active": bool, 
#                                          "Timestamp": datetime}, 
#                                 records=100)
# generator.generate_csv()