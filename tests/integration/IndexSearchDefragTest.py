import os
import json

from classes.API import StorageEngine
from classes.DataModels import Schema, DataWrite, DataDeletion, DataRetrieval, Condition, Operation
from classes.Types import IntType, VarCharType, FloatType
from classes.globals import INDEX_META_FILE, STATS_BASE_PATH

TABLE = "index_search_defrag_test"
DATA_FILE = f"storage/data/{TABLE}.dat"
STATS_FILE = f"{STATS_BASE_PATH}{TABLE}_stats.json"


def reset_environment():
    """Drop test table, clear data/stat files, and reset index metadata."""
    try:
        StorageEngine.drop_table(TABLE)
    except Exception:
        pass

    for path in (DATA_FILE, STATS_FILE):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        except Exception:
            pass

    try:
        with open(INDEX_META_FILE, "w") as f:
            json.dump({}, f, indent=2)
    except Exception:
        pass

    # Reset IndexController singleton
    from classes.Indexing.IndexController import IndexController
    IndexController._instance = None


def setup_table():
    """Create table with id, name, score columns."""
    schema = Schema(
        id=IntType(),
        name=VarCharType(30),
        score=FloatType(),
    )
    if not StorageEngine.create_table(TABLE, schema):
        return False, "Failed to create table"
    return True, ""


def insert_20_rows():
    """Insert 20 rows with sequential IDs."""
    rows = []
    for i in range(1, 21):
        rows.append([i, f"Student {i}", float(50 + i * 2)])

    request = DataWrite(
        TABLE,
        ["id", "name", "score"],
        [],
        rows,
    )
    StorageEngine.write_block(request)
    return True, ""


def create_index_on_id():
    """Create a unique B-tree index on the id column."""
    try:
        StorageEngine.set_index(TABLE, "id", "BTREE", True)
        return True, ""
    except Exception as e:
        return False, f"Failed to create index: {e}"


def search_with_index(id_value: int) -> tuple[bool, int, str]:
    """Search for a specific ID using the index."""
    req = DataRetrieval(
        TABLE,
        ["id", "name", "score"],
        [Condition(column="id", operation=Operation.EQ, operand=id_value)],
    )
    result = StorageEngine.read_block(req)
    count = result.row_count if hasattr(result, "row_count") else len(result.data)
    
    if count == 0:
        return False, 0, f"ID {id_value} not found"
    
    # Verify data
    row = result.data[0]
    actual_id = row[1] if len(row) > 3 else row[0]
    name = row[2] if len(row) > 3 else row[1]
    score = row[3] if len(row) > 3 else row[2]
    
    if actual_id != id_value:
        return False, count, f"ID mismatch: expected {id_value}, got {actual_id}"
    
    return True, count, f"Found: ID={actual_id}, Name={name}, Score={score}"


def delete_non_contiguous_rows():
    """Delete 5 non-contiguous rows: IDs 4, 8, 11, 15, 18."""
    to_delete = [4, 8, 11, 15, 18]
    deleted = 0
    for rid in to_delete:
        req = DataDeletion(
            TABLE,
            [Condition(column="id", operation=Operation.EQ, operand=rid)],
        )
        res = StorageEngine.delete_block(req)
        deleted += res if isinstance(res, int) else 0
    return True, f"Deleted {deleted} rows", deleted


def count_all_rows():
    """Count all rows in the table."""
    req = DataRetrieval(TABLE, ["id"], [])
    result = StorageEngine.read_block(req)
    return result.row_count if hasattr(result, "row_count") else len(result.data)


def search_multiple_ids(ids: list[int]) -> dict[int, bool]:
    """Search for multiple IDs and return which ones exist."""
    results = {}
    for id_val in ids:
        ok, count, _ = search_with_index(id_val)
        results[id_val] = (ok and count > 0)
    return results


def run_test():
    """Main test execution."""
    def test(success: bool, message: str) -> str:
        test.counter += 1
        if success:
            test.success += 1
            return f"TEST {test.counter} PASS: {message}"
        return f"TEST {test.counter} FAIL: {message}"

    test.counter = 0
    test.success = 0

    print("=== INDEX SEARCH + DEFRAG TEST ===\n")

    reset_environment()
    ok, msg = setup_table()
    print(test(ok, msg or "Table created"))

    # Insert 20 rows
    ok, msg = insert_20_rows()
    total = count_all_rows()
    print(test(ok and total == 20, f"Inserted 20 rows (found {total})"))

    # Create index
    ok, msg = create_index_on_id()
    print(test(ok, msg or "Index created on 'id' column"))

    # Search with index (before delete)
    test_ids_before = [1, 4, 8, 11, 15, 18, 20]
    before_results = search_multiple_ids(test_ids_before)
    all_found = all(before_results.values())
    print(test(all_found, f"Index search BEFORE delete: {sum(before_results.values())}/7 IDs found"))
    
    # Verification
    ok_detail, count_detail, msg_detail = search_with_index(10)
    print(test(ok_detail and count_detail == 1, f"  Verify ID=10: {msg_detail}"))

    # Delete 5 non-contiguous rows
    ok, msg, deleted = delete_non_contiguous_rows()
    total_after_delete = count_all_rows()
    print(test(ok and deleted == 5 and total_after_delete == 15, 
               f"{msg}, remaining={total_after_delete}"))

    # Search with index (after delete)
    after_delete_results = search_multiple_ids(test_ids_before)
    # IDs 1, 20 should exist; 4, 8, 11, 15, 18 should not
    expected_after_delete = {1: True, 4: False, 8: False, 11: False, 15: False, 18: False, 20: True}
    matches = all(after_delete_results[k] == v for k, v in expected_after_delete.items())
    print(test(matches, f"Index search AFTER delete: deleted IDs not found, others found"))
    
    # Verify a surviving ID
    ok_after, count_after, msg_after = search_with_index(10)
    print(test(ok_after and count_after == 1, f"  Verify ID=10 still exists: {msg_after}"))

    # Defragment
    defrag_ok = StorageEngine.defragment(TABLE)
    print(test(defrag_ok, "Defragmentation completed"))

    # Search with index (after defrag)
    total_after_defrag = count_all_rows()
    after_defrag_results = search_multiple_ids(test_ids_before)
    matches_defrag = all(after_defrag_results[k] == v for k, v in expected_after_delete.items())
    print(test(matches_defrag and total_after_defrag == 15, 
               f"Index search AFTER defrag: all expected IDs found correctly (total={total_after_defrag})"))
    
    # Final verification
    ok_final, count_final, msg_final = search_with_index(10)
    print(test(ok_final and count_final == 1, f"  Final verify ID=10: {msg_final}"))
    
    # Verify deleted IDs still don't exist
    ok_deleted, count_deleted, _ = search_with_index(8)
    print(test(not ok_deleted and count_deleted == 0, "  Verify deleted ID=8 still gone"))

    print(f"\n{'='*50}")
    print(f"{test.success}/{test.counter} tests passed.")
    print(f"{'='*50}")


if __name__ == "__main__":
    run_test()
