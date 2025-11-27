import os
from classes.Types import IntType, VarCharType, FloatType, CharType
from classes.DataModels import Schema, Statistic, DataRetrieval, Rows, Condition,Operation, DataWrite
from classes.Serializer import Serializer
from classes.IO import IO
from classes.API import StorageEngine
from classes.globals import CATALOG_FILE

def test_create_table():
    schemas_file = CATALOG_FILE
    if os.path.exists(schemas_file):
        os.remove(schemas_file)
    
    os.makedirs("storage", exist_ok=True)
    os.makedirs("storage/data", exist_ok=True)

    manager = StorageEngine() 

    print("--- Tes 1: Membuat tabel 'mahasiswa' ---")
    schema_mhs = Schema(
        id=IntType(),
        nama=VarCharType(50),
        ipk=FloatType()
    )
    
    success = manager.create_table("mahasiswa", schema_mhs)
    if success:
        print("BERHASIL!.")
    else:
        print("GAGAL.")

    print("\n--- Tes 2: Menambahkan tabel 'dosen' ---")
    schema_dosen = Schema(
        nidn=CharType(10),
        nama=VarCharType(100)
    )

    success = manager.create_table("dosen", schema_dosen)
    if success:
        print("BERHASIL!.")
    else:
        print("GAGAL.")

def test_drop_table():
    manager = StorageEngine()

    print("\n--- Tes 3: Menghapus tabel 'dosen' ---")
    success = manager.drop_table("student")
    if success:
        print("BERHASIL!.")
    else:
        print("GAGAL.")

def test_update_stats():
    s = Serializer()
    s.load_schema("student")
    storageIO = IO("student")
    
    dummy = [
        [101, "Alice Wonderland", 3.8],
        [102, "Bob Builder", 3.8],
        [103, "Charlie Chaplin", 3.9],
        [104, "David Beckham", 3.2],
        [105, "Eva Green", 4.0]
    ]

    data = s.serialize(dummy)
    print(data)

    storageIO.write(0, data)

    StorageEngine().update_stats("student")

    stat : Statistic = StorageEngine().get_stats("student")
    print(stat)

def test_read():
    # s = Serializer()
    # s.load_schema("student")
    # storageIO = IO("student")
    
    # dummy = [
    #     [101, "Alice Wonderland", 3.8],
    #     [102, "Bob Builder", 3.8],
    #     [103, "Charlie Chaplin", 3.9],
    #     [104, "David Beckham", 3.2],
    #     [105, "Eva Green", 4.0]
    # ]

    # data = s.serialize(dummy)
    # print(data)

    # storageIO.write(0, data)

    data_request : DataRetrieval = DataRetrieval("student", [], [Condition('ipk', Operation.GTE, 3.8)])
    res: Rows = StorageEngine.read_block(data_request)
    
    for i in range (res.row_count):
        print(res.data[i])

def test_write():
    #data dibawah ini gagal di write 
    # dummy_insert = [
    #     [101, "Alice Wonderland", 3.8],
    #     [102, "Bob Builder", 3.8],
    #     [103, "Charlie Chaplin", 3.9],
    #     [104, "David Beckham", 3.2],
    #     [105, "Eva Green", 4.0],

    #     [106, "John Doe", 3.6],
    #     [107, "Maria Hill", 3.7],
    #     [108, "Peter Parker", 3.5],
    #     [109, "Tony Stark", 3.9],
    #     [110, "Bruce Wayne", 3.4],
    #     [111, "Clark Kent", 3.1],
    #     [112, "Diana Prince", 3.9],
    #     [113, "Barry Allen", 3.3],
    #     [114, "Arthur Curry", 3.2],
    #     [115, "Natasha Romanoff", 3.8],
    #     [116, "Steve Rogers", 3.7],
    #     [117, "Wanda Maximoff", 3.9],
    #     [118, "Stephen Strange", 3.5]
    # ]

    #data dibawah ini kalo write berhasil tapi,kalo read error di deserialize
    dummy_insert = [
        [101, "Alice Wonderland", 3.8],
        [102, "Bob Builder", 3.8],
        [103, "Charlie Chaplin", 3.9],
        [104, "David Beckham", 3.2],
        [105, "Eva Green", 4.0],

        [106, "John Doe", 3.6],
        [107, "Maria Hill", 3.7],
        [108, "Peter Parker", 3.5],
        [109, "Tony Stark", 3.9],
        [110, "Bruce Wayne", 3.4],
        [111, "Clark Kent", 3.1],
        [112, "Diana Prince", 3.9],
        [113, "Barry Allen", 3.3],
        [114, "Arthur Curry", 3.2],
        [115, "Natasha Romanoff", 3.8],
        [116, "Steve Rogers", 3.7],
        [117, "Wanda Maximoff", 3.9],
        [118, "Stephen Strange", 3.5]
    ]

    #data ini memang berhasil write tapi entah kenapa dia jadi gagal read block kalo writeny dari storage engine
    #sementara kalo langsung dari IO bisa
    # dummy_insert = [
    #     [101, "Alice Wonderland", 3.8],
    #     [102, "Bob Builder", 3.8],
    #     [103, "Charlie Chaplin", 3.9],
    #     [104, "David Beckham", 3.2],
    #     [105, "Eva Green", 4.0]
    # ]
 
    data_request : DataWrite = DataWrite(
        "student",
        ["id","name","ipk"],
        [],
        dummy_insert
    )

    berhasil = StorageEngine.write_block(data_request)
    print("Row Affected " + str(berhasil))

if __name__ == "__main__":
    # Clean up previous test runs
    # if os.path.exists("storage/data/student.dat"):
        # os.remove("storage/data/student.dat")

    # run API tests
    # test_write_block_api()
    # test_read_block_api()
    # test_delete_block_api()
    # test_update_stats( )
    test_write()
    test_read()