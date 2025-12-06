"""
API.py (Working Title)

The main class that other components will call. Contains the storage engine class as shown in spec
"""

from classes.IO import IO
from classes.Serializer import Serializer, SerializerIncompleteBlockException
from classes.DataModels import DataRetrieval, DataWrite, DataDeletion, Condition, Statistic, Operation
from classes.DataModels import Schema,Rows
from classes.Indexing.IndexController import IndexController
from classes.Indexing.Index import Index, IndexPointer, IndexEntry, UniqueIndexViolationException
from classes.globals import CATALOG_FILE, BLOCK_SIZE, STATS_BASE_PATH, INDEX_META_FILE
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

    @staticmethod
    # Returns a list containing column names
    def load_schema_names(table_name: str) -> list[str]:
        serializer = Serializer()
        serializer.load_schema(table_name)
        return [col['name'] for col in serializer.schema['columns']]

    @staticmethod
    # list conditions dalam data retrieval panjang hanya 1 kalau ngga rusak ini
    def read_block(data_retrieval: DataRetrieval) -> Rows:
        table: str = data_retrieval.table
        io = IO(table)
        serializer = Serializer()
        serializer.load_schema(table)
        ic : IndexController = IndexController()

        mappingCol = StorageEngine.__create_column_mapping(serializer.schema["columns"])
        all_columns = [col["name"] for col in serializer.schema["columns"]]
        res: list[list[Any]] = []
        block_idx_gen = StorageEngine._sequential_search(io)

        idx_list: list[tuple[Condition, Index]] = []
        there_is_index = False
        for condition in data_retrieval.conditions:
            a = ic.get_index_for_table_column(table, condition.column)
            if a:
                there_is_index = True
            idx_list.append((condition, a))

        if(there_is_index):
            block_offset_mapping : Dict[int, list[int]]= {}
            unhandled_condition : list[Condition] = []
            data_in_block : list[list]= None
            list_offset_in_block = None

            for condition, index in idx_list:
                if(index):
                    it : Iterator[IndexEntry] = index.search_condition(condition)
                    for idx_entry in it:
                        idx_pointer = idx_entry.pointer
                        block_idx = idx_pointer.block_idx
                        print("ini block index di iteratornya " + str(block_idx))
                        offset = idx_pointer.offset
                        print("ini offset di iteratornya " + str(offset))
                        print()
                        block_offset_mapping.setdefault(block_idx, []).append(offset)
                else:
                    unhandled_condition.append(condition)


            for block_idx, list_offset in block_offset_mapping.items():
                raw_data_in_block : bytearray = io.read(block_idx)
                list_offset_in_block : list[int] = []

                while True:
                    try:
                        data_in_block = serializer.deserialize(raw_data_in_block, list_offset_in_block)
                        break
                    except SerializerIncompleteBlockException as e:
                        for _ in range(e.additional_needed_blocks):
                            next_idx = next(block_idx_gen, None)
                            if next_idx is None:
                                raise RuntimeError(
                                    "Unexpected EOF while reading multi-block record"
                                )

                            raw_data_in_block.extend(io.read(next_idx))

                schemaCols = StorageEngine.load_schema_names(table)
                for offset in (list_offset):
                    if offset in list_offset_in_block:  
                        valid_idx = list_offset_in_block.index(offset)
                        valid_data = data_in_block[valid_idx]

                        if data_retrieval.column:
                            projected_data = [
                                valid_data[col_idx]
                                for col_idx, colName in enumerate(schemaCols)
                                if colName in data_retrieval.column
                            ]
                            res.append(projected_data)
                        else:
                            res.append(valid_data)


        else:

            for idx in block_idx_gen:
                chunk = io.read(idx)
                if not chunk:
                    continue

                full_chunk = bytearray(chunk)
                data = None
                list_offset_in_block : list[int] = []
                while True:
                    try:
                        data = serializer.deserialize(full_chunk, list_offset_in_block)
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
                        op = condition.operation
                        try:
                            if isinstance(op, str):
                                op = Operation(op)
                            elif not isinstance(op, Operation):
                                op = Operation(op.value) if hasattr(op, 'value') else op
                        except Exception as e:
                            print(f"[DEBUG API] Error converting operation: {op}, type={type(op)}, error={e}")
                            raise
                        
                        func = StorageEngine.operation_funcs[op]

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
    
    @staticmethod
    def write_block(data_write: DataWrite) -> int:
        table: str = data_write.table
        serializer = Serializer()
        serializer.load_schema(table)

        inserted_values : list = []
        schema_columns : list = serializer.schema["columns"]
        
        input_col_map = {name: i for i, name in enumerate(data_write.column)}
        inc : int = 0
        next_row_id_in_stats = StorageEngine.get_next_row_id(table)

        for row in data_write.new_value:
            new_row : list = []
            for col in schema_columns:
                col_name = col["name"]
                if col_name in input_col_map:
                    val_idx = input_col_map[col_name]
                    new_row.append(row[val_idx])
                
                elif col_name == '__row_id':
                    new_row.append(next_row_id_in_stats + inc)
                elif col_name == "id":
                    new_row.append(0) 
                elif col["type"] == "int":
                    new_row.append(0)
                elif col["type"] == "float":
                    new_row.append(0.0)
                elif col["type"] == "char" or col["type"] == "varchar":
                    new_row.append("")
            
            inserted_values.append(new_row)
            inc += 1
        
        # Index pada tabel terkait
        index_controller: IndexController = IndexController()
        indices: list[tuple[int, Index]] = []
        for idx, col in enumerate(serializer.schema["columns"]):
            # NOTE: index hanya untuk satu kolom saat ini
            col_name = col["name"]
            index = index_controller.get_index_for_table_column(table, col_name)
            if index is not None:
                indices.append((idx, index))

        io = IO(table)
        last_block_idx : int = 1 + io.get_last_block_index()
        res : int = 0
        written_block_length : int = 0
        block : bytes = b""
        block_rows : int = 0
        def flush_block():
            nonlocal last_block_idx, res, written_block_length, block, block_rows
            length = io.write(last_block_idx, block[:BLOCK_SIZE])
            last_block_idx += length // BLOCK_SIZE   # some rows exceed block size
            res += block_rows

            written_block_length = written_block_length - BLOCK_SIZE
            block = block[BLOCK_SIZE:]
            block_rows = 0
        # Serialize per row: pack dalam satu blok dulu, lalu ke blok baru kalau melebihi block size
        row : int = 0
        while row < len(inserted_values):
            serialized_data : bytes = serializer.serialize([inserted_values[row]])
            serialized_data_length : int = len(serialized_data)
            
            # === Insert index
            for col_idx, index in indices:
                key = (inserted_values[row][col_idx],)
                pointer = IndexPointer(block_idx=last_block_idx, offset=written_block_length)
                entry = IndexEntry(key=key, pointer=pointer)
                try:
                    index.insert(entry)
                except UniqueIndexViolationException as e:
                    # Harusnya dirollback jika bahkan ada satu yang duplikat
                    print(f"{e}. Abort")
                    raise e

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

    @staticmethod
    def delete_block(data_deletion: DataDeletion) -> int:
        table: str = data_deletion.table
        io = IO(table)
        serializer = Serializer()
        serializer.load_schema(table)
        mappingCol = StorageEngine.__create_column_mapping(serializer.schema["columns"])

        # Index pada tabel terkait
        index_controller: IndexController = IndexController()
        indices: list[tuple[int, Index]] = []
        for idx, col in enumerate(serializer.schema["columns"]):
            # NOTE: index hanya untuk satu kolom saat ini
            col_name = col["name"]
            index = index_controller.get_index_for_table_column(table, col_name)
            if index is not None:
                indices.append((idx, index))
        
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

            # rows = serializer.deserialize(block)
            offsets: list[int] = []
            rows = serializer.deserialize(block, offsets)
            flag_delete = [False] * len(rows)

            for condition in data_deletion.conditions:
                colVal = mappingCol[condition.column]
                colIdx = colVal[0]
                colType = colVal[1]
                operand = condition.operand

                if(colType == 'float'):
                    b = struct.pack('!f', operand)         
                    x = struct.unpack('!f', b)[0] 
                    operand = x


                op = condition.operation
                try:
                    if isinstance(op, str):
                        op = Operation(op)
                    elif not isinstance(op, Operation):
                        op = Operation(op.value) if hasattr(op, 'value') else op
                except Exception as e:
                    print(f"[DEBUG API] Error converting operation: {op}, type={type(op)}, error={e}")
                    raise
                
                func = StorageEngine.operation_funcs[op]                

                for irow, row in enumerate(rows):
                    if flag_delete[irow]:
                        continue
                    if func(row[colIdx],operand):
                        flag_delete[irow] = True

            # === Delete index
            for i in range(len(rows)):
                if not flag_delete[i]:
                    continue
                for col_idx, index in indices:
                    # TODO: kalau non-unique index, harus tau block_idx dan offsetnya juga
                    key = (rows[i][col_idx],)
                    # pointer = None
                    pointer = IndexPointer(block_idx=idx, offset=offsets[i])
                    entry = IndexEntry(key=key, pointer=pointer)
                    index.delete(entry)
            
            new_rows = []
            for irow, row in enumerate(rows):
                if not flag_delete[irow]:
                    new_rows.append(row)
            res += sum(flag_delete)
            new_block = serializer.serialize(new_rows)

            if len(new_rows) == 0:
                new_block = b'\x00' * BLOCK_SIZE

            io.write(idx ,new_block)
            
            # === Update index entries for remaining rows with new offsets
            # After reorganizing the block, remaining rows have moved to new byte positions
            # We need to delete old entries and re-insert with new offsets
            if len(new_rows) > 0 and indices:
                # First delete old entries for remaining rows
                for i in range(len(rows)):
                    if flag_delete[i]:
                        continue  # Already deleted above
                    for col_idx, index in indices:
                        key = (rows[i][col_idx],)
                        pointer = IndexPointer(block_idx=idx, offset=offsets[i])
                        entry = IndexEntry(key=key, pointer=pointer)
                        index.delete(entry)
                
                # Then re-insert with new offsets
                new_offsets: list[int] = []
                serializer.deserialize(new_block, new_offsets)
                for i, row in enumerate(new_rows):
                    for col_idx, index in indices:
                        key = (row[col_idx],)
                        pointer = IndexPointer(block_idx=idx, offset=new_offsets[i])
                        entry = IndexEntry(key=key, pointer=pointer)
                        index.insert(entry)
            
            idx = next(block_idx_gen, None)
        
        return res


    def set_index(table: str, column:str, index_type: str, unique: bool = False) -> None:
        controller: IndexController = IndexController()
        controller.set_index(table, column, index_type, unique)

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
                
                # Rebuild all indexes for this table
                try:
                    StorageEngine._rebuild_indexes_for_table(table)
                except Exception as index_error:
                    print(f"Warning: Index rebuild failed during defragmentation: {index_error}")
                    print(f"Data file was successfully compacted, but indexes may be invalid.")
                    raise index_error
                
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
    def _rebuild_indexes_for_table(table: str) -> None:
        """
        Rebuilds all indexes for a table after defragmentation.
        This is necessary because defragmentation moves rows to new block/offset locations.
        
        Raises:
            Exception: If any index rebuild fails
        """
        try:
            index_controller = IndexController()
            serializer = Serializer()
            serializer.load_schema(table)
            
            # Load existing metadata to preserve it
            try:
                with open(INDEX_META_FILE, "r") as f:
                    index_metadata = json.load(f)
            except FileNotFoundError:
                index_metadata = {}
            
            # Count indexes to rebuild
            indexes_to_rebuild = [
                (index_name, index_meta) 
                for index_name, index_meta in index_controller.index_schema.items() 
                if index_meta["table"] == table
            ]
            
            if not indexes_to_rebuild:
                print(f"No indexes found for table '{table}'")
                return
            
            print(f"Rebuilding {len(indexes_to_rebuild)} index(es) for table '{table}'")
            
            # Rebuild each index
            failed_indexes = []
            for index_name, index_meta in indexes_to_rebuild:
                try:
                    index = index_controller.get_index(index_name)
                    if not index:
                        print(f"Warning: Could not load index object '{index_name}'")
                        failed_indexes.append((index_name, "Index object not found"))
                        continue
                    
                    print(f"Rebuilding index '{index_name}'")
                    
                    # Clear the index file before rebuilding
                    if os.path.exists(index.io.file_path):
                        os.remove(index.io.file_path)
                    
                    index.root = None
                    
                    # Rebuild from scratch
                    index.build_index(serializer)
                    index.load_metadata()
                    
                    # Preserve index metadata entry
                    if index_name in index_metadata:
                        # Keep the existing metadata (file path, table, columns, etc.)
                        pass
                    else:
                        # If not in metadata, add it
                        index_metadata[index_name] = {
                            "table": table,
                            "columns": index_meta.get("columns", []),
                            "index_file": index_meta.get("index_file", ""),
                            "type": index_meta.get("type", "BTREE"),
                            "unique": index_meta.get("unique", False),
                        }
                    
                    print(f"Index '{index_name}' rebuilt successfully.")
                    
                except Exception as e:
                    error_msg = f"Failed to rebuild index '{index_name}': {str(e)}"
                    print(f"Fail: {error_msg}")
                    failed_indexes.append((index_name, str(e)))
            
            # Write metadata back to ensure persistence
            try:
                with open(INDEX_META_FILE, "w") as f:
                    json.dump(index_metadata, f, indent=2)
            except Exception as e:
                print(f"Warning: Failed to write index metadata: {e}")
            
            # Report results
            if failed_indexes:
                error_list = "\n".join([f"    - {name}: {err}" for name, err in failed_indexes])
                raise RuntimeError(
                    f"Failed to rebuild {len(failed_indexes)} index(es):\n{error_list}"
                )
            
            print(f"Successfully rebuilt all {len(indexes_to_rebuild)} indexes for table '{table}'")
            
        except Exception as e:
            print(f"Error rebuilding indexes for table '{table}': {e}")
            raise

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