import json
import os
from classes.API import StorageEngine
from classes.DataModels import Schema, DataWrite, DataDeletion, DataRetrieval, Condition, Operation
from classes.Types import IntType, VarCharType, FloatType
from classes.globals import INDEX_META_FILE, STATS_BASE_PATH

TABLE = "defrag_index_test"
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


def setup_table_and_indexes():
    schema = Schema(
        id=IntType(),
        name=VarCharType(40),
        category=VarCharType(20),
        price=FloatType(),
    )
    if not StorageEngine.create_table(TABLE, schema):
        return False, "Failed to create table"

    StorageEngine.set_index(TABLE, "id", "BTREE", True)
    StorageEngine.set_index(TABLE, "category", "BTREE", False)
    return True, ""


def insert_20_rows():
    rows = []
    categories = ["catA", "catB", "catC", "catD"]
    for i in range(1, 21):
        cat = categories[(i - 1) % len(categories)]
        rows.append([i, f"Product {i}", cat, float(10 + i)])

    request = DataWrite(
        TABLE,
        ["id", "name", "category", "price"],
        [],
        rows,
    )
    StorageEngine.write_block(request)
    return True, ""


def delete_some_rows():
    to_delete = [3, 7, 12, 15, 18]
    deleted = 0
    for rid in to_delete:
        req = DataDeletion(
            TABLE,
            [Condition(column="id", operation=Operation.EQ, operand=rid)],
        )
        res = StorageEngine.delete_block(req)
        deleted += res if isinstance(res, int) else 0
    return True, "", deleted


def count_all_rows():
    req = DataRetrieval(TABLE, ["id"], [])
    result = StorageEngine.read_block(req)
    return result.row_count if hasattr(result, "row_count") else len(result.data)


def count_by_category(cat: str):
    req = DataRetrieval(
        TABLE,
        ["id", "category"],
        [Condition(column="category", operation=Operation.EQ, operand=cat)],
    )
    result = StorageEngine.read_block(req)
    return result.row_count if hasattr(result, "row_count") else len(result.data)


def count_by_category_fullscan(cat: str):
    """Full scan count for a category (bypasses index lookups)."""
    req = DataRetrieval(
        TABLE,
        ["id", "category"],
        [],
    )
    result = StorageEngine.read_block(req)
    data = result.data if hasattr(result, "data") else []
    return sum(1 for row in data if len(row) > 1 and row[1] == cat)


def exists_id(rid: int) -> bool:
    req = DataRetrieval(
        TABLE,
        ["id", "name"],
        [Condition(column="id", operation=Operation.EQ, operand=rid)],
    )
    result = StorageEngine.read_block(req)
    return (result.row_count if hasattr(result, "row_count") else len(result.data)) > 0


def search_and_verify_id(rid: int) -> tuple[bool, str]:
    """Search for a row by id and verify the returned data is correct."""
    req = DataRetrieval(
        TABLE,
        ["id", "name", "category", "price"],
        [Condition(column="id", operation=Operation.EQ, operand=rid)],
    )
    result = StorageEngine.read_block(req)
    data = result.data if hasattr(result, "data") else []
    
    if not data:
        return False, f"No rows found for id={rid}"
    
    row = data[0]
    # The columns are: [__row_id, id, name, category, price]
    row_id_val = row[0] if len(row) > 4 else None
    id_val = row[1] if len(row) > 4 else row[0]
    name_val = row[2] if len(row) > 4 else row[1]
    category_val = row[3] if len(row) > 4 else row[2]
    price_val = row[4] if len(row) > 4 else row[3]
    
    # Verify data is correct
    expected_name = f"Product {rid}"
    expected_category = ["catA", "catB", "catC", "catD"][(rid - 1) % 4]
    expected_price = float(10 + rid)
    
    if id_val != rid:
        return False, f"ID mismatch: expected {rid}, got {id_val}"
    if name_val != expected_name:
        return False, f"Name mismatch for id={rid}: expected '{expected_name}', got '{name_val}'"
    if category_val != expected_category:
        return False, f"Category mismatch for id={rid}: expected '{expected_category}', got '{category_val}'"
    if abs(price_val - expected_price) > 0.01:
        return False, f"Price mismatch for id={rid}: expected {expected_price}, got {price_val}'"
    
    return True, f"id={rid}: {name_val}, {category_val}, {price_val}"


def search_and_verify_category(cat: str) -> tuple[int, list[tuple[int, str]]]:
    """Search for rows by category and verify data, returning count and list of (id, name) tuples."""
    req = DataRetrieval(
        TABLE,
        ["id", "name", "category"],
        [Condition(column="category", operation=Operation.EQ, operand=cat)],
    )
    result = StorageEngine.read_block(req)
    data = result.data if hasattr(result, "data") else []
    
    verified = []
    for row in data:
        # The columns are: [__row_id, id, name, category]
        if len(row) > 3:
            id_val, name_val, cat_val = row[1], row[2], row[3]
        else:
            id_val, name_val, cat_val = row[0], row[1], row[2]
        
        if cat_val == cat:
            verified.append((id_val, name_val))
    
    return len(verified), verified


def run_tests():
    def record(success: bool, message: str) -> str:
        record.counter += 1
        if success:
            record.success += 1
            return f"TEST {record.counter} PASS: {message}"
        return f"TEST {record.counter} FAIL: {message}"

    record.counter = 0
    record.success = 0

    reset_environment()

    ok, msg = setup_table_and_indexes()
    print(record(ok, msg or "Table and indexes ready"))

    ok, msg = insert_20_rows()
    total_after_insert = count_all_rows()
    print(record(ok and total_after_insert == 20, f"Inserted 20 rows (found {total_after_insert})"))

    # Pre-delete index checks
    id_exists = exists_id(5)
    catC_count = count_by_category("catC")
    ok_search, msg_search = search_and_verify_id(5)
    print(record(id_exists and catC_count == 5 and ok_search, f"Index search before delete (id=5 verified: {msg_search}, catC count={catC_count})"))

    ok, msg, deleted = delete_some_rows()
    total_after_delete = count_all_rows()
    catC_after_delete_full = count_by_category_fullscan("catC")
    id5_still_exists = exists_id(5)
    id3_exists = exists_id(3)
    conditions_ok = (
        total_after_delete == 15
        and catC_after_delete_full == 2
        and id5_still_exists
        and not id3_exists
    )
    print(record(ok and conditions_ok, f"Deleted rows (deleted ~{deleted}), remaining={total_after_delete}, catC(full scan)={catC_after_delete_full}"))

    defrag_ok = StorageEngine.defragment(TABLE)
    print(record(defrag_ok, "Defragmentation completed"))

    # Post-defrag index checks
    total_after_defrag = count_all_rows()
    catC_after_defrag_idx = count_by_category("catC")
    catC_after_defrag_full = count_by_category_fullscan("catC")
    id5_exists_after = exists_id(5)
    id3_exists_after = exists_id(3)
    
    # Verify the index actually returns correct data
    ok_verify_id5, msg_verify_id5 = search_and_verify_id(5)
    catC_count_verify, catC_rows = search_and_verify_category("catC")
    
    post_ok = (
        total_after_defrag == 15
        and catC_after_defrag_full == 2
        and ok_verify_id5
        and catC_count_verify == 2
    )
    print(record(post_ok, f"Index search after defrag (rows={total_after_defrag}, catC idx/full={catC_after_defrag_idx}/{catC_after_defrag_full}, id5 verified={ok_verify_id5}, catC verified={catC_rows})"))

    print(f"{record.success}/{record.counter} tests passed.")


if __name__ == "__main__":
    run_tests()
