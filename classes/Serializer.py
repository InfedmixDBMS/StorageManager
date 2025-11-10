import json
import struct

SCHEMA_FILE = "storage/schemas.json"
from classes.IO import IO
from classes.globals import ROW_HEADER

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
                Bentuk tiap row adalah:
                    DELETE_FLAG + TOTAL_ROW_LEN + ROW
                Bentuk tiap kolom
                    DATA
                    LENGTH + DATA --> untuk varchar

                    DELETE_FLAG = "A" active, "D" deleted

        """
        if (not self.schema or self.schema == None):
            return b"\xde\xad\xc0\xde"

        bytes_data = [] # rows in bytes

        for tuple in data_list:
            prepared_values = []

            for column, value in zip(self.schema['columns'], tuple):
                if column['type'] == 'int':
                    packed_value = struct.pack('<i', value)
                    prepared_values.append(packed_value)

                elif column['type'] == 'float':
                    packed_value = struct.pack('<f', value)
                    prepared_values.append(packed_value)

                elif column['type'] == 'char':
                    value = str(value).encode('utf-8')
                    column_length = column['length']

                    if len(value) > column_length:
                        value = value[:column_length]
                    else:
                        value = value.ljust(column_length, '\x00')
                    
                    packed_value = value
                    prepared_values.append(packed_value)
                
                elif column['type'] == 'varchar':
                    value = str(value).encode('utf-8')
                    column_length = column['length']
                    data_length = len(value)

                    if data_length > column_length:
                        value = value[:column_length]
                        data_length = len(value)
                    
                    packed_length = struct.pack('<H', data_length)
                    packed_value = value
                    prepared_values.append(packed_length)
                    prepared_values.append(packed_value)

            tuple_data = b''.join(prepared_values)
            tuple_length = len(tuple_data)
            row_header = struct.pack(ROW_HEADER, b'A', tuple_length)
            bytes_data.append(row_header + tuple_data)

        return b''.join(bytes_data)



    def deserialize(self, raw_data: bytes) -> list[list]:
        if (not self.schema or self.schema == None):
            return b"\xde\xad\xc0\xde"

        pointer = 0
        data = []
        header_size = struct.calcsize(ROW_HEADER)

        while pointer < len(raw_data):
        # === HEADER PROCESSING
            if pointer + header_size > len(raw_data):
                break   

            tuple_header = raw_data[pointer : pointer+header_size]
            delete_flag, tuple_length = struct.unpack(ROW_HEADER, tuple_header)
            pointer += header_size

            if delete_flag == b"D":
                pointer += tuple_length
                continue

        # === BODY PROCESSING
            if pointer + tuple_length > len(raw_data):
                break

            tuple_data = raw_data[pointer : pointer+tuple_length]
            pointer += tuple_length

            tuple_pointer = 0
            tuple = []
            for col in self.schema['columns']:
                if col['type'] == 'int':
                    value = struct.unpack('<i', tuple_data[tuple_pointer : tuple_pointer + 4])[0]
                    tuple.append(value)
                    tuple_pointer += 4

                elif col['type'] == 'float':
                    value = struct.unpack('<f', tuple_data[tuple_pointer : tuple_pointer + 4])[0]
                    tuple.append(value)
                    tuple_pointer += 4

                elif col['type'] == 'char':
                    length = col['length']
                    value = tuple_data[tuple_pointer : tuple_pointer + length].rstrip(b'\x00').decode('utf-8')
                    tuple.append(value)
                    tuple_pointer += length

                elif col['type'] == 'varchar':
                    str_length = struct.unpack('<H', tuple_data[tuple_pointer : tuple_pointer + 2])[0]
                    tuple_pointer += 2
                    value = tuple_data[tuple_pointer : tuple_pointer + str_length].decode('utf-8')
                    tuple.append(value)
                    tuple_pointer += str_length

            data.append(tuple)
        return data

            



    def create_file(file_path: str):
        """
            Create file with header
        """
        pass



    def h_generate_header(self):
        pass

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




