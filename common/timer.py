import time

from loguru import logger

def calculate_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logger.info(f"Execution time of {func.__name__}: {(end_time - start_time)*1000} miliseconds")
        return result
    return wrapper