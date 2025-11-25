"""
IO.py (Working Title)

Fetch data in blocks, 
Ini class paling "low level" yang cuma ngebaca dan menulis ke blok
"""

from classes.globals import BLOCK_SIZE
import os

class IO:
    def __init__(self, table_name: str):
        self.file_path = f"storage/data/{table_name}.dat"

    def read(self, block_idx: int) -> bytes:
        with open(self.file_path, "rb") as f:
            f.seek(BLOCK_SIZE * block_idx)
            return f.read(BLOCK_SIZE)

    def write(self, block_idx: int, data: bytes) -> int:
        """
        data - serialized data
        """
        mode = "r+b" if os.path.exists(self.file_path) else "wb"
        
        # Correctly pad the data to the next full block boundary
        current_len = len(data)
        bytes_to_write = data
        if current_len % BLOCK_SIZE != 0:
            padding_needed = BLOCK_SIZE - (current_len % BLOCK_SIZE)
            bytes_to_write += b'\x00' * padding_needed

        with open(self.file_path, mode) as f:
            f.seek(BLOCK_SIZE * block_idx)
            return f.write(bytes_to_write)

    def delete(self, block_idx: int) -> int:
        """
            Ini cuma bakal di pake di defragmentasi, cuma ngehapus kalo semua data di blok tuh bener2 0 doang

            NOTE: kayaknya ga perlu sih ini, defragment full rewrite
        """
        pass

    def get_last_block_index(self) -> int:
        """
        get the index of the last block in file
        """
        try:
            stat = os.stat(self.file_path)  # From os metadata
            return (stat.st_size - 1) // BLOCK_SIZE
        except FileNotFoundError:
            return -1