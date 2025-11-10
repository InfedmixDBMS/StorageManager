"""
API.py (Working Title)

The main class that other components will call. Contains the storage engine class as shown in spec
"""

from classes.DataModels import DataRetrieval, DataWrite, DataDeletion, Condition, Statistic

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

    
    def get_stats() -> Statistic:
        """
            Returns a statistic object
        """
        pass

    # secara otomatis bakal ngelakuin vacuuming juga
    def defragment(table: str) -> bool:
        pass