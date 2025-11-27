import json
from classes.globals import INDEX_META_FILE
from classes.Indexing.IndexController import IndexController
from classes.Indexing.Index import IndexPointer, IndexEntry

def reset_index_metadata():
    with open(INDEX_META_FILE, "w") as f:
        json.dump({}, f, indent=2)

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
        print("Index created")
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
    messages = []
    messages.append(test(*simple_write_no_exception()))
    messages.append(test(*varchar_write_no_exception()))
    messages.append(test(*unique_write_and_delete_no_exception()))

    print("=== UNIT TESTING: Indexing Module ===")
    for message in messages:
        print(message)

    print(f"{test.success}/{len(messages)} tests passed.")

if __name__ == "__main__":
    test_all()
    # unique_write_and_delete_no_exception(reraise=True)