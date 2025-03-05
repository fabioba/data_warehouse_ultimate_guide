
import logging
logger = logging.getLogger(__name__)

# Define the task
def get_query(file_path: str):
    """
    The goal of this method is to read a query

    Args:  
        file_path (str): path of the query
    """
    logger.info(f'file_path: {file_path}')

    with open(file_path, "r") as file:
        return file.read()
