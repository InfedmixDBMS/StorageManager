"""
API.py (Working Title)

The main class that other components will call. Contains the storage engine class as shown in spec
"""

from classes.IO import IO
from classes.Serializer import Serializer, SerializerIncompleteBlockException
from classes.DataModels import DataRetrieval, DataWrite, DataDeletion, Condition, Statistic, Operation
from classes.DataModels import Schema,Rows
from classes.globals import CATALOG_FILE, BLOCK_SIZE, STATS_BASE_PATH
from typing import Dict, Iterator
import json
import operator
import os
import tempfile
import shutil
from typing import Any
import struct

class StorageEngine:
    operation_funcs : Dict = {
        Operation.EQ: operator.eq,
        Operation.NEQ: operator.ne,
        Operation.GT: operator.gt,
        Operation.GTE: operator.ge,
        Operation.LT: operator.lt,
        Operation.LTE: operator.le,
    }

    def read_block(data_retrieval: DataRetrieval) -> Rows:
        table = data_retrieval.table
        io = IO(table)
        serializer = Serializer()
        serializer.load_schema(table)

        mappingCol = StorageEngine.__create_column_mapping(serializer.schema["columns"])
        all_columns = [col["name"] for col in serializer.schema["columns"]]
        res: list[list[Any]] = []

        block_idx_gen = StorageEngine._sequential_search(io)

        for idx in block_idx_gen:
            chunk = io.read(idx)
            if not chunk:
                continue

            full_chunk = bytearray(chunk)
            data = None

            while True:
                try:
                    data = serializer.deserialize(full_chunk)
                    break

                except SerializerIncompleteBlockException as e:
                    for _ in range(e.additional_needed_blocks):
                        next_idx = next(block_idx_gen, None)
                        if next_idx is None:
                            raise RuntimeError(
                                "Unexpected EOF while reading multi-block record"
                            )

                        full_chunk.extend(io.read(next_idx))

            for row in data:
                passed = True
                for condition in data_retrieval.conditions:
                    colVal = mappingCol[condition.column]
                    colIdx = colVal[0]
                    colType = colVal[1]
                    func = StorageEngine.operation_funcs[condition.operation]
                    operand = condition.operand

                    if(colType == 'float'):
                        b = struct.pack('!f', operand)         
                        x = struct.unpack('!f', b)[0] 
                        operand = x
                    if not func(row[colIdx], operand):
                        passed = False
                        break

                if passed:
                    if data_retrieval.column:
                        projected_row = [row[mappingCol[col][0]] for col in data_retrieval.column]
                        res.append(projected_row)
                    else:
                        res.append(row)

        return Rows(
            columns=data_retrieval.column if data_retrieval.column else all_columns,
            data=res
        )
    
    def write_block(data_write: DataWrite) -> int:
        table: str = data_write.table
        serializer = Serializer()
        serializer.load_schema(table)

        inserted_values : list = []
        inserted_columns : set = data_write.column
        schema_columns : list = serializer.schema["columns"]
        inc : int = 0
        next_row_id_in_stats = StorageEngine.get_next_row_id(table)
        for row in data_write.new_value:
            new_row : list = []
            i_idx : int = 0
            sch_idx : int = 0
            while sch_idx < len(schema_columns):
                col = schema_columns[sch_idx]
                if i_idx < len(inserted_columns) and col["name"] == inserted_columns[i_idx]:  # Provided column
                    new_row.append(row[i_idx])
                    i_idx += 1

                # Imputation
                # TODO: column generator, mungkin default value atau inkremen suatu sequence
                elif col['name'] == '__row_id':
                    new_row.append(next_row_id_in_stats + inc)
                elif col["name"] in ["id"]:  # Auto increment id if insert
                    # NOTE: Karena update bakal diimplementasi sebagai DELETE -> INSERT, kolom ini gaboleh ga diinsert
                    new_row.append(0)   # TODO: implement auto increment, perhaps from statistics
                elif col["type"] == "int":
                    new_row.append(0)
                elif col["type"] == "float":
                    new_row.append(0.0)
                elif col["type"] == "char" or col["type"] == "varchar":
                    new_row.append("")
                sch_idx += 1
            inserted_values.append(new_row)
            inc+=1

        io = IO(table)
        last_block_idx : int = 1 + io.get_last_block_index()
        res : int = 0
        written_block_length : int = 0
        block : bytes = b""
        block_rows : int = 0
        def flush_block():
            nonlocal last_block_idx, res, written_block_length, block, block_rows
            
            # TODO: Update index here
            
            length = io.write(last_block_idx, block)
            last_block_idx += length // BLOCK_SIZE   # some rows exceed block size
            res += block_rows

            written_block_length = 0
            block = b""
            block_rows = 0
        # Serialize per row: pack dalam satu blok dulu, lalu ke blok baru kalau melebihi block size
        row : int = 0
        while row < len(inserted_values):
            serialized_data : bytes = serializer.serialize([inserted_values[row]])
            serialized_data_length : int = len(serialized_data)
            # TODO: Check for unique/primary key constraint violation here with index

            if written_block_length + serialized_data_length > BLOCK_SIZE:
                flush_block()

            written_block_length += serialized_data_length
            block += serialized_data
            block_rows += 1

            if row == len(data_write.new_value) - 1 and written_block_length > 0:
                flush_block()
            row += 1

        # Update max_row_id 
        if len(inserted_values) > 0:
            max_inserted_id = max(row[0] for row in inserted_values)
            current_max = StorageEngine.get_next_row_id(table) - 1
            if max_inserted_id > current_max:
                StorageEngine._update_max_row_id(table, max_inserted_id)

        return res


    def delete_block(data_deletion: DataDeletion) -> int:
        table: str = data_deletion.table
        io = IO(table)
        serializer = Serializer()
        serializer.load_schema(table)

        res : int = 0
        
        # TODO: Algorithm beda kalau ada indeks
        block_idx_gen = StorageEngine._sequential_search(io)
        idx = next(block_idx_gen, None)
        while idx is not None:
            try:
                block = io.read(idx)
            except SerializerIncompleteBlockException as e:
                for _ in range(e.additional_needed_blocks):
                    idx = next(block_idx_gen, None)
                    if idx is None: # Abnormal
                        return res
                    block += io.read(idx)

            rows = serializer.deserialize(block)
            flag_delete = [False] * len(rows)

            for condition in data_deletion.conditions:
                colIdx : int = serializer.schema["columns"].find(condition.column)
                func = StorageEngine.operation_funcs[condition.operation]
                for irow, row in enumerate(rows):
                    if flag_delete[irow]:
                        continue
                    if func(row[colIdx], condition.operand):
                        flag_delete[irow] = True

            # TODO: Update index
            
            new_rows = []
            for irow, row in enumerate(rows):
                if not flag_delete[irow]:
                    new_rows.append(row)
            res += sum(flag_delete)
            new_block = serializer.serialize(new_rows)
            io.write(new_block)
            idx = next(block_idx_gen, None)
        
        return res


    def set_index(table: str, column:str, index_type: str) -> None:
        pass


    # TODO: create sama drop masih soft delete (fileny gak di delete)
    # TODO: ini gatau bakal jadi pake class Schema atau enggak
    @staticmethod
    def create_table(table_name: str, schema: Schema) -> bool:
        column_list = [
            {"name":name, **dtype.to_dict()} for name, dtype in schema.columns.items()
        ]

        new_schema : Dict = {
            "file_path": f"storage/data/{table_name}.dat",
            "row_size": schema.size,
            "columns": column_list
        }
        
        try:
            data = json.load(open(CATALOG_FILE, "r"))
            data[table_name] = new_schema
            with open(CATALOG_FILE, "w") as f:
                json.dump(data, f, indent=2)
            return True
        
        except FileNotFoundError:
            print(f"File not found. Creating a new one with 'enrollment' table.")
            with open(CATALOG_FILE, 'w') as f:
                json.dump({table_name: new_schema}, f, indent=2)
            return True

        except Exception as e:
            print(f"An error occurred: {e}")
            return False
        
    @staticmethod
    def drop_table(table_name: str) -> bool:
        try:
            with open(CATALOG_FILE, "r") as f:
                data = json.load(f)

            if table_name in data:
                del data[table_name]
            else:
                print("Table not found.")
                return False
            
            with open(CATALOG_FILE, "w") as f:
                json.dump(data, f, indent=2)
            print(f"Table {table_name} dropped successfully.")
            return True
        except FileNotFoundError:
            print(f"Catalog file {CATALOG_FILE} not found.")
        except Exception as e:
            print(f"An error occurred: {e}")


    # secara otomatis bakal ngelakuin vacuuming juga
    @staticmethod
    def defragment(table: str) -> bool:
        """
        Return true if successful.
        """
        try:
            serializer = Serializer()
            serializer.load_schema(table)
            io = IO(table)

            # Check row yang tidak terhapus
            active_rows : list[list] = []
            max_row_id : int = -1
            block_iterator = StorageEngine._sequential_search(io)
            
            while True:
                idx = next(block_iterator, None)
                if idx is None:
                    break
                
                try:
                    block = io.read(idx)
                    rows = serializer.deserialize(block)
                    for row in rows:
                        if row[0] > max_row_id:
                            max_row_id = row[0]
                    active_rows.extend(rows)
                except SerializerIncompleteBlockException as e:
                    for _ in range(e.additional_needed_blocks):
                        idx = next(block_iterator, None)
                        if idx is None:
                            break
                        block += io.read(idx)
                    rows = serializer.deserialize(block)
                    for row in rows:
                        if row[0] > max_row_id:
                            max_row_id = row[0]
                    active_rows.extend(rows)
                except Exception as e:
                    print(f"Error reading block {idx}: {e}")
                    continue

            # Buat temp file
            temp_fd, temp_path = tempfile.mkstemp(suffix='.dat', dir='storage/data')
            try:
                os.close(temp_fd)
                
                temp_io = IO.__new__(IO)
                temp_io.file_path = temp_path
                
                # Tulis ke temp file
                if len(active_rows) > 0:
                    block_idx : int = 0
                    current_block : bytes = b""
                    current_block_size : int = 0

                    for row in active_rows:
                        serialized_row = serializer.serialize([row])
                        row_size = len(serialized_row)

                        if current_block_size + row_size > BLOCK_SIZE:
                            temp_io.write(block_idx, current_block)
                            block_idx += 1
                            current_block = b""
                            current_block_size = 0

                        current_block += serialized_row
                        current_block_size += row_size

                    if current_block_size > 0:
                        temp_io.write(block_idx, current_block)
                
                # Ganti file asli dengan tempfile
                if os.path.exists(io.file_path):
                    os.remove(io.file_path)
                shutil.move(temp_path, io.file_path)
                
                # Update max_row_id
                StorageEngine._update_max_row_id(table, max_row_id)
                
                print(f"Defragmentation completed for table '{table}'. Active rows: {len(active_rows)}")
            except Exception as e:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                raise e
            return True

        except FileNotFoundError:
            print(f"Table file for '{table}' not found.")
            return False
        except Exception as e:
            print(f"Error during defragmentation: {e}")
            return False

    # karena return value nya class Statistic, ini return satu table aja, kalo mau multiple table handle di caller aj
    @staticmethod
    def get_stats(table: str) -> Statistic:
        try:
            with open(STATS_BASE_PATH + table + "_stats.json", "r") as f:
                stats_data = json.load(f)
                statistic = Statistic(
                    n_r=stats_data["n_r"],
                    b_r=stats_data["b_r"],
                    f_r=stats_data["f_r"],
                    l_r=stats_data["l_r"],
                    V_a_r=stats_data["V_a_r"]
                )
                return statistic

        except Exception as e:
            print("Unexcpected error in get_stats(): ", e)

    @staticmethod
    def get_next_row_id(table: str) -> int:
        try:
            with open(STATS_BASE_PATH + table + "_stats.json", "r") as f:
                stats_data = json.load(f)
                max_row_id = stats_data.get("max_row_id", -1)
                return max_row_id + 1
        except FileNotFoundError:
            # Stats file doesn't exist, return 0
            return 0
        except Exception as e:
            print(f"Error in get_next_row_id: {e}")
            return 0

    @staticmethod
    def _update_max_row_id(table: str, max_row_id: int) -> None:
        """
        Updates the max_row_id in statistics file.
        Creates the file if it doesn't exist.
        """
        try:
            file_path = STATS_BASE_PATH + table + "_stats.json"
            
            try:
                with open(file_path, "r") as f:
                    stats = json.load(f)
            except FileNotFoundError:
                stats = {
                    "n_r": 0,
                    "b_r": 0,
                    "f_r": 0,
                    "l_r": 0,
                    "V_a_r": {},
                    "max_row_id": -1
                }
            
            # Update max_row_id
            stats["max_row_id"] = max_row_id
            
            # Write back
            with open(file_path, "w") as f:
                json.dump(stats, f, indent=2)
        except Exception as e:
            print(f"Error updating max_row_id: {e}")

    @staticmethod
    def update_stats(table: str = "all") -> None:
        """
            Updates the statistics file for the given table
            This function recalculates the statistics from the data file
        """

        if table == "all":
            catalog = json.load(open(CATALOG_FILE, "r"))
            for table in catalog.keys():
                print("LOG: updating stats for table", table)
                StorageEngine.update_stats(table)
            return

        serializer = Serializer()
        serializer.load_schema(table)
        io = IO(table)

        block_iterator = StorageEngine._sequential_search(io)
        nr : int = 0 # number of tuples
        br : int = io.get_last_block_index() + 1 # number of blocks containing tuples
        unique_values : dict[str, set] = [set() for column in serializer.schema["columns"] if column["name"] != "__s"]
        v_a_r : dict[str, int] = {}
        max_row_id : int = -1

        # size of tuple ambil average tuple size
        total_row_size : int = 0

        while True:
            idx = next(block_iterator, None)
            if idx is None:
                break
            block = io.read(idx)
            rows = serializer.deserialize(block)
            for row in rows:
                total_row_size += serializer.get_row_size(row)
                for col, value in enumerate(row):
                    unique_values[col].add(value)
                if row[0] > max_row_id:
                    max_row_id = row[0]
                nr += 1


        fr : int = nr // br if br > 0 else 0   # blocking factor
        lr : int = total_row_size // nr if nr > 0 else 0 # avg size of tuple
        for i, col in enumerate(serializer.schema["columns"]):
            column_name = col["name"]
            if column_name != "__row_id":
                v_a_r[column_name] = len(unique_values[i])

        stats =  {
            "n_r": nr,
            "b_r": br,
            "f_r": fr,
            "l_r": lr,
            "V_a_r": v_a_r,
            "max_row_id": max_row_id
        }

        file_path = STATS_BASE_PATH + table + "_stats.json"
        try:
            with open(file_path, "w") as f:
                json.dump(stats, f, indent=2)
            print(f"LOG: Statistics for table {table} updated successfully.")
        except Exception as e:
            print("Unexpected error")


    #Helper method
    @staticmethod
    def __create_column_mapping(columns: list[dict]) -> dict[str, int]:
        print(columns)
        mapping = {}
        for i, col in enumerate(columns):
            mapping[col["name"]] = (i,col['type'])
        return mapping

    # def update_stats


    # --- SCAN ALGORITHMS ---
    # Algorithm A1: Ful table scan
    @staticmethod
    def _sequential_search(file_io: IO) -> Iterator[int]:
        """
        Returns an iterator over all the table block indices
        """
        yield from range(1 + file_io.get_last_block_index())