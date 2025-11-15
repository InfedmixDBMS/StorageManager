from abc import ABC, abstractmethod
from dataclasses import dataclass
import os
from typing import Generic, Iterator, TypeVar
from enum import Enum
from classes.IO import IO
from classes.Serializer import Serializer, SerializerIncompleteBlockException
from classes.DataModels import Condition
from classes.Types import CharType, DataType, FloatType, IntType, VarCharType
from classes.globals import INDEX_META_FILE, DATA_STORE_PATH
from classes.BTreeIndex import BTreeIndex
import json

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
    def __init__(self, file_path: str, table: str, columns: list[str], key_type: tuple[DataType], unique: bool):
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
    def insert(self, key: K, block_index: int, offset: int):
        pass

    @abstractmethod
    def delete(self, key: K):
        pass

    @abstractmethod
    def search(self, key: K) -> Iterator[IndexEntry[K]]:
        pass

    @abstractmethod
    def search_condition(self, condition: Condition) -> Iterator[IndexEntry[K]]:
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
        col_idx : int = serializer.schema["columns"].index(self.columns[0])
        
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
                self.insert(key, index_block, offsets[i])
                res += 1
            block_idx += 1
        
        return res

class IndexController:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if getattr(self, "_initialized", False):
            return

        self.index_map: dict[str, Index] = {}
        self.index_schema = json.load(open(INDEX_META_FILE, "r"))
        self._initialize_indexes()

        self._initialized = True

    def get_index(self, name: str) -> Index | None:
        """
        Mengembalikan objek index berdasarkan nama
        """
        return self.index_map.get(name, None)
    
    def get_index_for_table_column(self, table: str, column: str) -> Index | None:
        """
        Mengembalikan objek index yang terkait dengan table.column jika ada.
        """
        for meta_name, meta in self.index_schema.items():
            if meta["table"] == table and column in meta["column"]:
                return self.index_map[meta_name]
        return None

    def set_index(self, table: str, column: str, key_type: str, index_type: str, is_unique: bool, **kwargs) -> int:
        """
        Membuat index baru pada table.column
        """
        index_name = f"{table}_{column}_{index_type}"
        if index_name in self.index_map:
            raise ValueError(f"Index {index_name} sudah ada.")

        # Create index file
        try:
            file_path = os.path.join(DATA_STORE_PATH, f"{index_name}.idx")
            open(file_path, 'wb').close()
        except Exception as e:
            print(f"An error occurred while creating index file: {e}")
            raise e

        
        # Create index object
        serializer = Serializer()
        key_type: list[DataType] = []
        serializer.load_schema(table)
        for col in serializer.schema["columns"]:
            if col["name"] == column:   # Support key satu kolom dulu
                if col["type"] == "int":
                    key_type.append(IntType()),
                elif col["type"] == "float":
                    key_type.append(FloatType()),
                elif col["type"] == "char":
                    key_type.append(CharType(col["length"])),
                elif col["type"] == "varchar":
                    key_type.append(VarCharType(col["length"])),
        if index_type == IndexType.BTREE:
            index_object = BTreeIndex(file_path=file_path,
                                      table=table,
                                      columns=[column],
                                      key_type=tuple(key_type),
                                      unique=is_unique)
        elif index_type == IndexType.HASH:
            index_object = None
            # TODO: Create hash index
            raise NotImplementedError("Hash index belum diimplementasi")
        else:
            raise ValueError(f"Invalid index type {index_type}")
        self.index_map[index_name] = index_object
        index_object.build_index(serializer)
        
        # Update metadata
        new_index_schema = {
            "file_path": file_path,
            "table": table,
            "column": [column],
            "key_type": key_type,
            "unique": is_unique,
            "type": index_type.value,
            **kwargs
        }
        try:
            self.index_schema[index_name] = new_index_schema
            with open(INDEX_META_FILE, "w") as f:
                json.dump(self.index_schema, f, indent=2)
        except Exception as e:
            print(f"An error occurred while updating index metadata: {e}")
            del self.index_map[index_name]
            os.remove(file_path)
            raise e

        return 0

    def _initialize_indexes(self):
        """
        For each index in the schema, create the appropriate Index object.
        """
        for index_name, meta in self.index_schema.items():
            type = meta["type"].upper()

            # TODO: populate self.index_map
            if type == IndexType.BTREE:
                self.index_map[index_name] = BTreeIndex(**meta)
            elif type == IndexType.HASH:
                pass
            else:
                raise ValueError(f"Unknown index method: {type}")