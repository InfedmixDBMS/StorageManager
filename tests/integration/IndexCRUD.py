import json
import os
import glob
from classes.Indexing.IndexController import IndexController
from classes.Indexing.Index import IndexPointer, IndexEntry, UniqueIndexViolationException
from classes.DataModels import Schema, Statistic, DataRetrieval, Rows, Condition,Operation, DataWrite, DataDeletion
from classes.globals import INDEX_META_FILE
from classes.API import StorageEngine



def setup_test():
    file_patterns = [
        "storage/data/*.dat",
        "storage/index/*.dat",
        "storage/statistics/*.json",
    ]
    for pattern in file_patterns:
        for file_path in glob.glob(pattern):
            try:
                os.remove(file_path)
            except OSError as e:
                print(f"Error removing file {file_path}: {e}")

    with open(INDEX_META_FILE, "w") as f:
        json.dump({}, f, indent=2)
    
    controller = IndexController()
    StorageEngine.set_index("student", "id", "BTREE", True)
    StorageEngine.set_index("course", "year", "BTREE")

def write_on_indexed_table(reraise: bool = False) -> tuple[bool, str]:
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

    data_request : DataWrite = DataWrite(
        "student",
        ["id","name","ipk"],
        [],
        dummy_insert
    )

    try:
        StorageEngine.write_block(data_request)
        return True, ""
    except Exception as e:
        if reraise:
            raise e
        return False, f"Exception occurred: {e}"

def write_duplicate_on_unique_index(reraise: bool = False) -> tuple[bool, str]:
    dummy_insert = [
        [201, "Alice Wonderland", 3.8],
        [202, "Bob Builder", 3.8],
        [203, "Charlie Chaplin", 3.9],
        [204, "David Beckham", 3.2],
        [205, "Eva Green", 4.0],
        [201, "Duplicate Alice", 3.5]  # Duplicate primary key
    ]

    data_request : DataWrite = DataWrite(
        "student",
        ["id","name","ipk"],
        [],
        dummy_insert
    )

    try:
        StorageEngine.write_block(data_request)
        return False, "Expected exception for duplicate key, but write succeeded."
    except UniqueIndexViolationException as e:
        return True, ""
    except Exception as e:
        if reraise:
            raise e
        return False, f"Unexpected exception occurred: {e}"

def delete_on_indexed_table(reraise: bool = False) -> tuple[bool, str]:
    data_request : DataDeletion = DataDeletion(
        "student",
        [
            Condition(
                column="id",
                operation=Operation.EQ,
                operand=102
            )
        ]
    )

    try:
        StorageEngine.delete_block(data_request)
        return True, ""
    except Exception as e:
        if reraise:
            raise e
        return False, f"Exception occurred: {e}"

def write_non_unique(reraise: bool = False) -> tuple[bool, str]:
    dummy_insert = [
        [301, 2025, "IF2224", "Automata theory"],
        [302, 2025, "IF3140", "Sistem Basis Data"],
        [303, 2024, "IF2110", "Data Structures"],
        [304, 2025, "IF2230", "Computer Networks"],
        [305, 2024, "IF2150", "Operating Systems"],
    ]

    data_request : DataWrite = DataWrite(
        "course",
        ["id","year","code","description"],
        [],
        dummy_insert
    )

    try:
        StorageEngine.write_block(data_request)
        return True, ""
    except Exception as e:
        if reraise:
            raise e
        return False, f"Exception occurred: {e}"

def read_with_index(reraise: bool = False) -> tuple[bool, str]:
    try:
        for target_id in range(101, 119):
            
            condition = Condition(column="id", operation=Operation.EQ, operand=target_id)
            
            data_request = DataRetrieval(
                table="student",
                column=["id", "name", "ipk"],
                conditions=[condition]
            )
            
            result_rows = StorageEngine.read_block(data_request)
            
            # print(f"{target_id}")
            # continue

            if result_rows.row_count != 1:
                return False, f"ID {target_id}: Expected 1 row, but got {result_rows.row_count}"
                
            student_data = result_rows.to_dict()[0]
            
            if student_data["id"] != target_id:
                return False, f"ID {target_id}: Logic Error. Requested {target_id} but returned {student_data['id']}"

            if target_id == 105:
                expected_data = {"name": "Eva Green", "ipk": 4.0}
                if student_data["name"] != expected_data["name"] or abs(student_data["ipk"] - expected_data["ipk"]) > 1e-9:
                    return False, f"ID 105: Data mismatch. Expected {expected_data}, got {student_data}"

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

    setup_test()
    test.counter = 0
    test.success = 0
    messages = []
    messages.append(test(*write_on_indexed_table()))
    # messages.append(test(*delete_on_indexed_table()))
    # messages.append(test(*write_non_unique()))
    # messages.append(test(*write_duplicate_on_unique_index()))
    messages.append(test(*read_with_index()))

    print("=== INTEGRATION TESTING: Indexing with API ===")
    for message in messages:
        print(message)

    print(f"{test.success}/{len(messages)} tests passed.")

if __name__ == "__main__":
    test_all()