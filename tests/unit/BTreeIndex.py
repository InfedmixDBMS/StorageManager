import json
import os
from classes.globals import INDEX_META_FILE
from classes.Indexing.IndexController import IndexController
from classes.Indexing.Index import Index, IndexPointer, IndexEntry
from classes.DataModels import Condition, Operation

def reset_index_metadata():
    # Clear all idx files
    meta = json.load(open(INDEX_META_FILE, "r"))
    for index_name in meta.keys():
        file_name: str = meta[index_name]["file_path"]
        # Delete file
        try:
            os.remove(file_name)
        except Exception:
            pass
    with open(INDEX_META_FILE, "w") as f:
        json.dump({}, f, indent=2)
    
    IndexController._instance = None

def simple_write_no_exception(reraise: bool = False) -> tuple[bool, str]:
    reset_index_metadata()
    index_controller = IndexController()

    try:
        index_controller.set_index("student", "id", "BTREE", True)
        index = index_controller.get_index_for_table_column("student", "id")
        for i in range(100000):
            tup = (i,)
            pointer = IndexPointer(block_idx=23+i, offset=(2+i)%512)
            entry = IndexEntry(key=tup, pointer=pointer)
            if not index.insert(entry):
                break
        return True, ""
    except Exception as e:
        if reraise:
            raise e
        return False, f"Exception occurred: {e}"


def varchar_write_no_exception(reraise: bool = False) -> tuple[bool, str]:
    reset_index_metadata()
    index_controller = IndexController()

    try:
        index_controller.set_index("course", "description", "BTREE", False)
        index = index_controller.get_index_for_table_column("course", "description")
        for i in range(5):
            tup = (f"123456789012345678901234567890" * 120,)
            pointer = IndexPointer(block_idx=45+i, offset=(3+i)%512)
            entry = IndexEntry(key=tup, pointer=pointer)
            if not index.insert(entry):
                break
        return True, ""
    except Exception as e:
        if reraise:
            raise e
        return False, f"Exception occurred: {e}"

def unique_write_and_delete_no_exception(reraise: bool = False) -> tuple[bool, str]:
    reset_index_metadata()
    index_controller = IndexController()

    try:
        index_controller.set_index("course", "id", "BTREE", True)
        index = index_controller.get_index_for_table_column("course", "id")
        for i in range(1000):
            tup = (i,)
            pointer = IndexPointer(block_idx=10+i, offset=(5+i)%512)
            entry = IndexEntry(key=tup, pointer=pointer)
            if not index.insert(entry):
                break
        for i in range(0, 1000, 2):
            tup = (i,)
            pointer = IndexPointer(block_idx=10+i, offset=(5+i)%512)
            entry = IndexEntry(key=tup, pointer=pointer)
            if not index.delete(entry):
                break
        return True, ""
    except Exception as e:
        if reraise:
            raise e
        return False, f"Exception occurred: {e}"

def write_and_retrieve_unique(reraise: bool = False) -> tuple[bool, str]:
    reset_index_metadata()
    index_controller = IndexController()

    try:
        index_controller.set_index("course", "year", "BTREE", True)
        index = index_controller.get_index_for_table_column("course", "year")
        for i in range(1000):
            tup = (i,)
            pointer = IndexPointer(block_idx=15+i, offset=(7+i)%512)
            entry = IndexEntry(key=tup, pointer=pointer)
            if not index.insert(entry):
                break
        # Retrieve test
        for i in range(1000):
            tup = (i,)
            results = list(index.search(tup))
            if len(results) == 0:
                return False, f"Failed to retrieve entry for key {tup}"
            if results[0].pointer.block_idx != 15+i or results[0].pointer.offset != (7+i)%512:
                return False, f"Incorrect pointer for key {tup}"
        return True, ""
    except Exception as e:
        if reraise:
            raise e
        return False, f"Exception occurred: {e}"

def write_and_retrieve_duplicate(reraise: bool = False) -> tuple[bool, str]:
    reset_index_metadata()
    index_controller = IndexController()

    try:
        index_controller.set_index("course", "year", "BTREE", False)
        index: Index = index_controller.get_index_for_table_column("course", "year")

        tup = (2,)
        entries: int = 1000
        pointers_set = set(range(entries))
        for i in range(entries):
            pointer = IndexPointer(i, offset=2)
            entry = IndexEntry(key=tup, pointer=pointer)
            if not index.insert(entry):
                raise RuntimeError("Unable to insert key")

        # Retrieve test
        retrieved_entries = index.search(tup)
        for entry in retrieved_entries:
            if entry.pointer.block_idx not in pointers_set:
                return False, f"Failed to retrieve pointer {entry.pointer} for key {tup}"
            pointers_set.remove(entry.pointer.block_idx)

        if len(pointers_set) != 0:
            return False, "Unable to retrieve all index entries"
        return True, ""
    except Exception as e:
        if reraise:
            raise e
        return False, f"Exception occurred: {e}"

def write_delete_retrieve_unique(reraise: bool = False) -> tuple[bool, str]:
    reset_index_metadata()
    index_controller = IndexController()

    try:
        index_controller.set_index("course", "year", "BTREE", True)
        index: Index = index_controller.get_index_for_table_column("course", "year")
        entries_count: int = 20000

        # Insert entries
        for i in range(entries_count):
            tup = (i,)
            pointer = IndexPointer(block_idx=20, offset=12)
            entry = IndexEntry(key=tup, pointer=pointer)
            if not index.insert(entry):
                raise RuntimeError("Unable to insert key")

        # Delete entries
        for i in range(0, entries_count, 2):
            tup = (i,)
            pointer = IndexPointer(block_idx=20, offset=12)
            entry = IndexEntry(key=tup, pointer=pointer)
            delete_success: bool = index.delete(entry)
            if not delete_success:
                raise RuntimeError("Unable to delete key")

        # Retrieve test
        for i in range(entries_count):
            tup = (i,)
            results = list(index.search(tup))
            if i % 2 == 0:
                if len(results) != 0:
                    return False, f"Deleted key {tup} was still found in index"
            else:
                if len(results) == 0:
                    return False, f"Failed to retrieve entry for key {tup}"
                if results[0].pointer.block_idx != 20 or results[0].pointer.offset != 12:
                    return False, f"Incorrect pointer for key {tup}"
        return True, ""
    except Exception as e:
        if reraise:
            raise e
        return False, f"Exception occurred: {e}"

def write_delete_retrieve_duplicate(reraise: bool = False) -> tuple[bool, str]:
    reset_index_metadata()
    index_controller = IndexController()

    try:
        index_controller.set_index("course", "year", "BTREE", False)
        index: Index = index_controller.get_index_for_table_column("course", "year")

        tup = (3,)
        entries: int = 10000
        pointers_set = set(range(entries))
        # Insert entries
        for i in range(entries):
            pointer = IndexPointer(i, offset=3)
            entry = IndexEntry(key=tup, pointer=pointer)
            if not index.insert(entry):
                raise RuntimeError("Unable to insert key")

        # Delete half entries
        for i in range(0, entries, 2):
            pointer = IndexPointer(i, offset=3)
            entry = IndexEntry(key=tup, pointer=pointer)
            if not index.delete(entry):
                raise RuntimeError("Unable to delete key")

        # Retrieve test
        retrieved_entries = index.search(tup)
        for entry in retrieved_entries:
            if entry.pointer.block_idx not in pointers_set:
                return False, f"Failed to retrieve pointer {entry.pointer} for key {tup}"
            if entry.pointer.block_idx % 2 == 0:
                return False, f"Deleted pointer {entry.pointer} was still found in index"
            pointers_set.remove(entry.pointer.block_idx)

        for i in range(0, entries, 2):
            if i in pointers_set:
                return False, f"Unable to retrieve pointer with block_idx {i}"

        return True, ""
    except Exception as e:
        if reraise:
            raise e
        return False, f"Exception occurred: {e}"

def test_all():
    def test(success: bool, message: str) -> str:
        test.counter += 1
        if success:
            test.success += 1
            return f"TEST {test.counter} SUCCESS {message}"
        else:
            return f"TEST {test.counter} FAILED {message}"

    test.counter = 0
    test.success = 0
    tests = [simple_write_no_exception,
             varchar_write_no_exception,
             unique_write_and_delete_no_exception,
             write_and_retrieve_unique,
             write_and_retrieve_duplicate,
             write_delete_retrieve_unique,
             write_delete_retrieve_duplicate]

    print("=== UNIT TESTING: Indexing Module ===")
    for t in tests:
        print(test(*t()))

    print(f"{test.success}/{len(tests)} tests passed.")

if __name__ == "__main__":
    test_all()
    # print(write_and_retrieve_duplicate(reraise=True))
    
    # print(write_delete_retrieve_unique(reraise=False))

    # keys = []
    # ic = IndexController()
    # index = ic.get_index_for_table_column("course", "year")
    # for entry in index.search_condition(Condition("year", Operation.LTE, 2600)):
    #     if entry.key[0] < 2400: continue
    #     keys.append(entry.key)
    # print(keys)