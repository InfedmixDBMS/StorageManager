"""
API.py (Working Title)

The main class that other components will call. Contains the storage engine class as shown in spec
"""

from temporary.InputObjects import DataRetrieval, DataWrite, DataDeletion, Condition

class StorageEngine:
    def read_block(data_retrieval: DataRetrieval) -> list[list]:
        """
            Returns rows
        """
        pass
    
    def write_block(data_write: DataWrite) -> int:
        """
            Returns number of rows affected
        """
        pass
    
    def delete_block(data_deletion: DataDeletion) -> int:
        """
            Returns number of rows affected
        """
        pass

    def set_index(table: str, column:str, index_type: str) -> None:
        pass

    
    def get_stats():
        """
            Returns a statistic object
        """
        pass

    def defragment(table: str):
        pass