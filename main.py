from classes.IO import IO
from classes.Serializer import Serializer




if __name__ == "__main__":
    s = Serializer()
    s.load_schema("student")
    storageIO = IO(s.schema["file_path"])
    
    dummy = [
        [2147483647, "Alif", 2.3],
        [2147483647, "Alif", 2.3],
        [2147483647, "Alif", 2.3],
        [2147483647, "Alif", 2.3],
        ]

    data = s.serialize(dummy)
    storageIO.write(0, data)
    read_data = storageIO.read(0)
    print(read_data)