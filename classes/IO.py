"""
IO.py (Working Title)

Fetch data in blocks, 
Ini class paling "low level" yang cuma ngebaca dan menulis ke blok
"""

from classes.globals import BLOCK_SIZE

class IO:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def read(self, block_idx: int) -> bytes:
        with open(self.file_path, "rb") as f:
            f.seek(BLOCK_SIZE * block_idx)
            return f.read(BLOCK_SIZE)

    def write(self, block_idx: int, data: bytes) -> int:
        """
        data - serialized data
        """
        with open(self.file_path, "wb") as f:
            f.seek(BLOCK_SIZE * block_idx)
            f.write(data.ljust(BLOCK_SIZE, b'\x00'))

    def delete(self, block_idx: int) -> int:
        """
            Ini cuma bakal di pake di defragmentasi, cuma ngehapus kalo semua data di blok tuh bener2 0 doang

            NOTE: kayaknya ga perlu sih ini, defragment full rewrite
        """
        pass