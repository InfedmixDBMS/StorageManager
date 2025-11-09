"""
IO.py (Working Title)

Fetch data in blocks, 
"""

class IO:
    def __init__(self, file_path):
        self.file_path = file_path

    def read(self, block_idx: int, byte_offset: int) -> bytes:
        pass

    def write(self, block_idx: int, data: bytes):
        """
        data - deserialized data
        """
        pass