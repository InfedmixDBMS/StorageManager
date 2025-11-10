import json
import struct

SCHEMA_FILE = "storage/schemas.json"
from classes.IO import IO

class Serializer:
    def __init__(self):
        self.schema = []
        self.json = json.load(open(SCHEMA_FILE, "r"))


    def load_schema(self, table_name : str) -> None:
        """
            Loads a schema from json file into the schema attribute based on table name
        """
        self.schema = self.json[table_name]
        print(self.schema)

    def serialize(self, data_list : list[list]) -> bytes:
        """
            Serializes python data list into bytes, based on schema map
            Params:
                data_list: a list of rows of data ex: [['Alice', 101, 3.8], ['Bob', 102, 3.5]]
            
            Note:
                data harus datang terurut berdasarkan kolom sesuai yang ada pada schemas.json
        """
        if (not self.schema or self.schema == None):
            return b"\xde\xad\xc0\xde"

        fmt = self.h_build_format_string()
        print(f"Format: " + fmt)
        data_rows = [] # rows in bytes

        for row in data_list:
            prepared_values = []

            for schema_column, value in zip(self.schema['columns'], row):
                if schema_column['type'] in ['int', 'float']:
                    prepared_values.append(value)
                
                # TODO: handle varchar correctly
                elif schema_column['type'] in ['char', 'varchar']:
                    target_length = schema_column['length']
                    value_bytes = str(value).encode('utf-8')

                    if len(value_bytes) > target_length:
                        value_bytes = value_bytes[:target_length]
                    else:
                        value_bytes = value_bytes.ljust(target_length, b'\x00')

                    prepared_values.append(value_bytes)
            
            row_bytes = struct.pack(fmt, *prepared_values)
            data_rows.append(row_bytes)

        return b''.join(data_rows)



    def deserialize(self, raw_data: bytes) -> list[list]:
        pass

    def create_file(file_path: str):
        """
            Create file with header
        """
        pass

    def h_generate_header(self):
        pass

    def h_build_format_string(self):
        fmt = "<" # little-endian

        for col in self.schema['columns']:
            if col['type'] == "int":
                fmt += 'i'
            elif col['type'] == "float":
                fmt += 'f'
            elif col['type'] == "char":
                fmt += f"{col['length']}s"
        # TODO: differentiate char and varchar
        # varchar will have a 4 byte metadata for the length, for now (max length - MAXINT)
            elif col['type'] == "varchar":
                fmt += f"{col['length']+4}s"

        return fmt

if __name__ == "__main__":
    s = Serializer()
    s.load_schema("mahasiswa")
    io = IO(s.schema["file_path"])
    
    dummy = [
        [2147483647, "Alif", "13523045", 2.3],
        [2147483647, "Alif", "13523045", 2.3],
        [2147483647, "Alif", "13523045", 2.3],
        [2147483647, "Alif", "13523045", 2.3],
        ]

    data = s.serialize(dummy)
    IO.write(data)




    