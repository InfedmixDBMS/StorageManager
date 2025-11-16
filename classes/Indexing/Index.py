from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Iterator, TypeVar
from enum import Enum
from classes.IO import IO
from classes.Serializer import Serializer, SerializerIncompleteBlockException
from classes.DataModels import Condition
from classes.Types import DataType

K = TypeVar("K", bound=tuple)  # Index Key type

class IndexType(Enum):
    BTREE = "BTREE"
    HASH = "HASH"

@dataclass
class IndexPointer:
    block_idx: int
    offset: int

@dataclass
class IndexEntry(Generic[K]):
    key: K
    pointer: IndexPointer

class UniqueIndexViolationException(Exception):
    def __init__(self, message: str):
        super().__init__(message)

class Index(ABC, Generic[K]):
    def __init__(self, file_path: str, table: str, columns: list[str], key_type: tuple[DataType], unique: bool, **kwargs):
        self.table: str = table
        self.columns: list[str] = columns
        if isinstance(key_type, tuple):
            self.key_types: tuple[DataType] = key_type
        else:
            self.key_types: tuple[DataType] = (key_type,)
        self.unique: bool = unique
        self.io: IO = IO(file_path)

        if len(self.key_types) == 0:
            raise ValueError("[StorageManager] Key types must be specified for BTreeIndex initialization")
        if len(self.key_types) != len(self.columns):
            raise ValueError("[StorageManager] Key types count must match columns count for BTreeIndex initialization")
        if len(self.key_types) > 1000:
            raise ValueError("[StorageManager] Key columns count exceeds maximum limit of 1000")

    @abstractmethod
    def load_metadata(self):
        """
        Load metadata dari file index, termasuk header sesuai kebutuhan.
        """
        pass

    @abstractmethod
    def insert(self, key: K, pointer: IndexPointer):
        """ 
        Insert entry ke index
        Params:
            key: Key dari entry yang akan dimasukkan
            pointer: Pointer ke lokasi data sebenarnya
        """
        pass

    @abstractmethod
    def delete(self, key: K, specific_entry_pointer: IndexPointer | None = None) -> int:
        """
        Delete semua entry dengan key tertentu dari index.
        Params:
            key: Key dari entry yang akan dihapus
            specific_entry_pointer: Jika diberikan, hanya hapus entry dengan pointer tersebut.
        Returns:
            Jumlah entry yang dihapus
        """
        pass

    @abstractmethod
    def search(self, key: K) -> Iterator[IndexEntry[K]]:
        """
        Mengembalikan iterator dari semua entry dengan key tertentu.
        Params:
            key: Key dari entry yang akan dicari
        """
        pass

    @abstractmethod
    def search_condition(self, condition: Condition) -> Iterator[IndexEntry[K]]:
        """
        Mengembalikan iterator dari semua entry yang memenuhi kondisi tertentu.
        Params:
            condition: Kondisi yang harus dipenuhi oleh entry
        """
        
        pass

    @abstractmethod
    def _initialize_index_file(self):
        """
        Inisialisasi file index, termasuk header sesuai kebutuhan.
        """
        pass

    def build_index(self, serializer: Serializer) -> int:
        """
        Membangun index file baru untuk suatu tabel pada satu kolom.
        Implementasi naif sequential search dan insert satu persatu.
        Params:
            serializer: Serializer yang sudah dimuat dengan schema tabel terkait
        """
        self._initialize_index_file()

        data_io = IO(serializer.schema["file_path"])
        col_idx: int = 0
        for i, col in enumerate(serializer.schema["columns"]):
            if col["name"] == self.columns[0]:
                col_idx = i
                break
        
        res : int = 0

        block_idx : int = 0
        last_idx : int = data_io.get_last_block_index()
        while block_idx <= last_idx:
            block_data: bytes = b""
            index_block: int = block_idx
            offsets: list[int] = []

            try:
                block_data = data_io.read(block_idx)
                rows = serializer.deserialize(block_data, offsets)
            except SerializerIncompleteBlockException as e:
                for _ in range(e.additional_needed_blocks):
                    block_idx += 1
                    if block_idx > last_idx:
                        return
                    block_data += data_io.read(block_idx)
                rows = serializer.deserialize(block_data, offsets)
            
            for i, row in enumerate(rows):
                key = row[col_idx]
                self.insert(key, IndexPointer(block_idx=index_block, offset=offsets[i]))
                res += 1
            block_idx += 1


        return res