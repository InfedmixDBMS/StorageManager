"""
API.py (Working Title)

The main class that other components will call. Contains the storage engine class as shown in spec
"""

from classes.IO import IO
from classes.Serializer import Serializer
from classes.DataModels import DataRetrieval, DataWrite, DataDeletion, Condition, Statistic, Operation
from classes.DataModels import Schema
from classes.globals import CATALOG_FILE
from typing import Dict
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
        pass
    
    def delete_block(data_deletion: DataDeletion) -> int:
        """
            Returns number of rows affected
        """
        pass

    def set_index(table: str, column:str, index_type: str) -> None:
        pass

    # TODO: create sama drop masih soft delete (fileny gak di delete)
    def create_table(self, table_name: str, schema: Schema) -> bool:
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
        
    def drop_table(self, table_name: str) -> bool:
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

    def get_stats(table: str = "all") -> Statistic:
        """
            Returns a statistic object
        """
        pass

    #Helper method
    def __create_column_mapping(self,columns: list[dict]) -> dict[str, int]:
        mapping = {}
        for i, col in enumerate(columns):
            mapping[col["name"]] = i
        return mapping

    # def update_stats