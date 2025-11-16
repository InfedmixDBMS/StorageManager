import os
import json
from classes.globals import INDEX_META_FILE, DATA_STORE_PATH
from classes.Serializer import Serializer
from classes.Types import DataType, IntType, FloatType, CharType, VarCharType
from classes.Indexing.Index import Index, IndexType
from classes.Indexing.BTreeIndex import BTreeIndex

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

    def set_index(self, table: str, column: str, index_type: str, is_unique: bool, **kwargs) -> int:
        """
        Membuat index baru pada table.column
        """
        index_name = f"{table}_{column}_{index_type}"
        index_file = os.path.join(DATA_STORE_PATH, f"{index_name}.idx")
        if index_name in self.index_map:
            raise ValueError(f"Index {index_name} sudah ada.")

        # Create index file
        try:
            open(index_file, 'wb').close()
        except Exception as e:
            print(f"An error occurred while creating index file: {e}")
            raise e

        
        # Create index object
        serializer = Serializer()
        key_types: list[DataType] = []
        key_types_str: list[str] = []
        serializer.load_schema(table)
        for col in serializer.schema["columns"]:
            if col["name"] == column:   # Support key satu kolom dulu
                if col["type"] == "int":
                    key_types.append(IntType())
                    key_types_str.append("int")
                elif col["type"] == "float":
                    key_types.append(FloatType())
                    key_types_str.append("float")
                elif col["type"] == "char":
                    key_types.append(CharType(col["length"]))
                    key_types_str.append("char")
                elif col["type"] == "varchar":
                    key_types.append(VarCharType(col["length"]))
                    key_types_str.append("varchar")
        if index_type == IndexType.BTREE.value:
            index_object = BTreeIndex(file_path=index_file,
                                      table=table,
                                      columns=[column],
                                      key_type=tuple(key_types),
                                      unique=is_unique)
        elif index_type == IndexType.HASH.value:
            index_object = None
            # TODO: Create hash index
            raise NotImplementedError("Hash index belum diimplementasi")
        else:
            raise ValueError(f"Invalid index type {index_type}")
        self.index_map[index_name] = index_object
        index_object.build_index(serializer)
        index_object.load_metadata()
        
        # Update metadata
        new_index_schema = {
            "file_path": index_file,
            "table": table,
            "columns": [column],
            "key_type": key_types_str,
            "unique": is_unique,
            "type": index_type,
            **kwargs
        }
        try:
            self.index_schema[index_name] = new_index_schema
            with open(INDEX_META_FILE, "w") as f:
                json.dump(self.index_schema, f, indent=2)
        except Exception as e:
            print(f"An error occurred while updating index metadata: {e}")
            del self.index_map[index_name]
            os.remove(index_file)
            raise e

        return 0

    def _initialize_indexes(self):
        """
        For each index in the schema, create the appropriate Index object.
        """
        for index_name, meta in self.index_schema.items():
            type = meta["type"].upper()

            # TODO: populate self.index_map
            if type == IndexType.BTREE.value:
                self.index_map[index_name] = BTreeIndex(**meta)
            elif type == IndexType.HASH.value:
                pass
            else:
                raise ValueError(f"Unknown index method: {type}")
            
            self.index_map[index_name].load_metadata()
            
if __name__ == "__main__":
    from classes.Indexing.Index import IndexPointer
    with open(INDEX_META_FILE, "w") as f:
        json.dump({}, f, indent=2)
    index_controller = IndexController()
    # print(io.write(0, b"Hello, World!"))
    # print(io.read(0))
    index_controller.set_index("student", "id", "BTREE", True)
    index = index_controller.get_index("student_id_BTREE")
    index.insert((12345,), IndexPointer(block_idx=1, offset=0))