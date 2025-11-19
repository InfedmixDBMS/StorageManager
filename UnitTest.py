import os
from classes.Types import IntType, VarCharType, FloatType, CharType
from classes.DataModels import Schema
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
        [0 ,101, "Alice Wonderland", 3.8],
        [1 ,102, "Bob Builder", 3.8],
        [2 ,103, "Charlie Chaplin", 3.9],
        [3 ,104, "David Beckham", 3.2],
        [4 ,105, "Eva Green", 4.0]
    ]

    data = s.serialize(dummy)
    print(data)

    storageIO.write(0, data)

    StorageEngine().update_stats("student")

    stat : Statistic = StorageEngine().get_stats("student")
    print(stat)

if __name__ == "__main__":
    # run API tests
    # test_write_block_api()
    # test_read_block_api()
    # test_delete_block_api()
    test_update_stats( )