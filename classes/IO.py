"""
IO.py (Working Title)

Fetch data in blocks, 
Ini class paling "low level" yang cuma ngebaca dan menulis ke blok
"""

from classes.globals import BLOCK_SIZE, FILE_METADATA_SIZE
import os
import struct

class IO:
    def __init__(self, table_name: str):
        self.file_path = f"storage/data/{table_name}.dat"

    # ngebuat file dengan menginit metadata (0 dulu)
    def create_file(self):
        """
        NOTE: metadata untuk sekarang adalah sebagai berikut
        row_id : 2 bytes

        Yang berarti, sisanya kosong aja
        """
         # Init row_id = 0 (2 bytes unsigned short), sisa 14 bytes null padding
        metadata = struct.pack('<H', 0) + (b'\x00' * (FILE_METADATA_SIZE - 2))
        
        with open(self.file_path, "wb") as f:
            f.write(metadata)


    def read(self, block_idx: int) -> bytes:
        with open(self.file_path, "rb") as f:
            f.seek((BLOCK_SIZE * block_idx) + FILE_METADATA_SIZE)
            return f.read(BLOCK_SIZE)

    def write(self, block_idx: int, data: bytes) -> int:
        """
        data - serialized data
        """
        if not os.path.exists(self.file_path):
            self.create_file()
        mode = "r+b" if os.path.exists(self.file_path) else "wb"
        
        # Correctly pad the data to the next full block boundary
        current_len = len(data)
        bytes_to_write = data
        if current_len % BLOCK_SIZE != 0:
            padding_needed = BLOCK_SIZE - (current_len % BLOCK_SIZE)
            bytes_to_write += b'\x00' * padding_needed

        with open(self.file_path, mode) as f:
            f.seek((BLOCK_SIZE * block_idx) + FILE_METADATA_SIZE)
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
        
    # TODO: pindahin ke tempat yang semestinya
    def get_last_row_id(self) -> int:
        if not os.path.exists(self.file_path):
            return -1
        with open(self.file_path, "rb") as f:
            data = f.read(2)
            return struct.unpack('<H', data)[0]
    
    def update_last_row_id(self, new_id: int) -> bool:
        if not os.path.exists(self.file_path):
            return False
        
        try:
            with open(self.file_path, "r+b") as f:
                f.write(struct.pack('<H'), new_id)
                if self.get_last_row_id() == new_id:
                    return True
                else:
                    return False
        except:
            return False