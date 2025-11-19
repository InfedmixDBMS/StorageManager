"""
API.py (Working Title)

The main class that other components will call. Contains the storage engine class as shown in spec
"""

from scipy import stats
from classes.IO import IO
from classes.Serializer import Serializer, SerializerIncompleteBlockException
from classes.DataModels import DataRetrieval, DataWrite, DataDeletion, Condition, Statistic, Operation
from classes.DataModels import Schema
from classes.globals import CATALOG_FILE, BLOCK_SIZE, STATS_BASE_PATH
from typing import Dict, Iterator
import json
import operator

class StorageEngine:
    operation_funcs : Dict = {
        Operation.EQ: operator.eq,
        Operation.NEQ: operator.ne,
        Operation.GT: operator.gt,
        Operation.GTE: operator.ge,
        Operation.LT: operator.lt,
        Operation.LTE: operator.le,
    }

    def read_block(self, data_retrieval: DataRetrieval) -> list[list]:
        """
        Returns rows that satisfy given conditions
        """
        table: str = data_retrieval.table
        io = IO(table)
        serializer = Serializer()
        serializer.load_schema(table)

        mappingCol = self.__create_column_mapping(serializer.schema["columns"])
        res: list[list] = []  

        idx : int = 0
        #TODO: Implement kalau ada index di colnya


        while True:
            chunk: bytes = io.read(idx)
            if not chunk:  # EOF
                break

            data = serializer.deserialize(chunk)
            for row in data:
                passed : bool = True
                for condition in data_retrieval.conditions:
                    colIdx = mappingCol[condition.column]
                    func = self.operation_funcs[condition.operation]  
                    operand = condition.operand

                    if not func(row[colIdx], operand):
                        passed = False
                        break  

                if passed:
                    if data_retrieval.column:  #kalau pengen early projection columnya isi aj
                        projected_row = [row[mappingCol[col]] for col in data_retrieval.column]
                        res.append(projected_row)
                    else:
                        res.append(row)

            idx += 1

        return res  
    
    def write_block(data_write: DataWrite) -> int:
        """
            Returns number of rows affected
        """
        table: str = data_write.table
        serializer = Serializer()
        serializer.load_schema(table)

        inserted_values : list = []
        inserted_columns : set = data_write.column
        schema_columns : list = serializer.schema["columns"]
        for row in data_write.new_value:
            new_row : list = []
            i_idx : int = 0
            sch_idx : int = 0
            while sch_idx < len(schema_columns):
                col = schema_columns[sch_idx]
                if col["name"] == inserted_columns[i_idx]:  # Provided column
                    new_row.append(row[i_idx])
                    i_idx += 1

                # Imputation
                # TODO: column generator, mungkin default value atau inkremen suatu sequence
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

        io = IO(serializer.schema["file_path"])
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

        return res


    def delete_block(data_deletion: DataDeletion) -> int:
        """
            Returns number of rows affected
        """
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
    def defragment(table: str) -> bool:
        pass

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
                nr += 1


        fr : int = nr // br if br > 0 else 0   # blocking factor
        lr : int = total_row_size // nr if nr > 0 else 0 # avg size of tuple
        for i, col in enumerate(serializer.schema["columns"]):
            column_name = col["name"]
            if column_name != "__special_row_id":
                v_a_r[column_name] = len(unique_values[i])

        stats =  {
            "n_r": nr,
            "b_r": br,
            "f_r": fr,
            "l_r": lr,
            "V_a_r": v_a_r
        }

        file_path = STATS_BASE_PATH + table + "_stats.json"
        try:
            with open(file_path, "w") as f:
                json.dump(stats, f, indent=2)
            print(f"LOG: Statistics for table {table} updated successfully.")
        except Exception as e:
            print("Unexpected error")


    #Helper method
    def __create_column_mapping(self,columns: list[dict]) -> dict[str, int]:
        mapping = {}
        for i, col in enumerate(columns):
            mapping[col["name"]] = i
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