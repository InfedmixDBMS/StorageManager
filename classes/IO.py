"""
IO.py (Working Title)

Fetch data in blocks, 
"""

from classes.globals import BLOCK_SIZE

class IO:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.file_handle = open(file_path, 'rb')

    def read(self, block_idx: int) -> bytes:
        self.file_handle.seek(BLOCK_SIZE * block_idx)
        return self.file_handle.read(BLOCK_SIZE)

    def write(self, block_idx: int, data: bytes) -> int:
        """
        data - deserialized data
        """
        pass

    def delete(self, block_idx: int) -> int:
        pass