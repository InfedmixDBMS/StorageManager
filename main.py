from classes.IO import IO
from classes.Serializer import Serializer




if __name__ == "__main__":
    s = Serializer()
    s.load_schema("student")
    storageIO = IO(s.schema["file_path"])
    
    dummy = [
        [0 ,101, "Alice Wonderland", 3.8],
        [1 ,102, "Bob Builder", 3.5],
        [2 ,103, "Charlie Chaplin", 3.9],
        [3 ,104, "David Beckham", 3.2],
        [4 ,105, "Eva Green", 4.0]
    ]

    data = s.serialize(dummy)
    print(data)

    storageIO.write(0, data)
    data = storageIO.read(0)

    deserialized = s.deserialize(data)
    print("\n")
    print(deserialized)