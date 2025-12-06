# Infedmix-StorageManager

This project is a custom storage manager component built with Python. It provides a low-level storage engine for creating, reading, writing, and deleting records, with support for indexing and basic data management.

## 📋 Table of Contents

- [How It Works](#-how-it-works)
- [Core Concepts](#-core-concepts)
  - [Record & Block Structure](#record--block-structure)
  - [Metadata Management](#metadata-management)
  - [Data Types](#data-types)
- [Current Features](#-current-features)
- [Project Team](#-project-team)
- [Open Questions](#-open-questions)

## ⚙️ How It Works

The Storage Manager provides a `StorageEngine` class as the main public API for all database operations. Here's a brief overview of its architecture:

1.  **Table Storage**: Each table is stored as a separate `.dat` file in the `storage/data/` directory.
2.  **Block-Based I/O**: Data files are read and written in fixed-size chunks called **blocks** (1024 bytes). The `IO` class handles these low-level file operations.
3.  **Serialization**: The `Serializer` class is responsible for converting Python objects into a compact byte format for storage and deserializing them back on reads. It uses a schema to correctly pack and unpack different data types.
4.  **Record Management**:
    -   **Writes**: New records are typically appended to the end of the table file.
    -   **Deletes**: Records are not immediately purged. Instead, a "delete flag" in the record's header is set. A `defragment` operation is available to physically remove these records and compact the file.
    -   **Reads**: Data can be read sequentially or through an index for faster lookups. The engine supports filtering records based on conditions.
5.  **Indexing**: The system supports creating indexes (currently B-Tree) on columns to accelerate data retrieval. The `IndexController` manages the lifecycle of these indexes.

> [!NOTE]
> The engine is designed to handle records that are larger than a single block by spanning them across multiple consecutive blocks.

## 🧩 Core Concepts

### Record & Block Structure

Each record (row) is stored with a header that contains metadata about the record itself.

```
[      ROW HEADER      ] [                         TUPLE DATA (BODY)                        ]
+----------------------+ +--------------------+---------------+-----------------+-----------+
| Flag (1B) | Len (2B) | |  __row_id (4B)     |   INT (4B)    |    VARCHAR(N)   | FLOAT(4B) |
+----------------------+ +--------------------+---------------+-----------------+-----------+
```

-   **Flag**: A 1-byte flag indicating the record's status (`A` for Active, `D` for Deleted).
-   **Len**: A 2-byte integer specifying the total length of the tuple data (body).
-   **`__row_id`**: An internal, auto-generated unique identifier for each row.

### Metadata Management

The system relies on JSON files for managing metadata:

-   `storage/catalog.json`: Contains the schema for each table, including column names, data types, and file paths.
-   `storage/index.json`: Contains metadata for all created indexes, such as the table and column they belong to, their type (e.g., BTREE), and file path.
-   `storage/statistics/`: This directory holds statistics for each table, like the number of records, which can be used for query optimization.

### Data Types

The serializer supports a set of primitive data types:
-   `int`: 4-byte signed integer.
-   `float`: 4-byte floating-point number.
-   `char`: Fixed-length character string, padded with null bytes.
-   `varchar`: Variable-length character string, prefixed with a 2-byte length metadata.

> [!IMPORTANT]
> For `varchar` columns, if the input data exceeds the defined maximum length, it will be truncated upon insertion.

## ✨ Current Features

-   **CRUD Operations**: Full support for creating, reading, and deleting records.
-   **Schema Management**: Ability to `create_table` and `drop_table`.
-   **Serialization/Deserialization**: Robust conversion between in-memory objects and byte format.
-   **Indexing**: B-Tree indexing is implemented for faster queries.
-   **Defragmentation**: A manual `defragment` process is available to reclaim space from deleted records.
-   **Statistics**: A utility to `update_stats` for tables is available.

## 👥 Project Team

| NAMA | NIM | GitHub |
| :--- | :--- | :--- |
| M. Rayhan Farrukh | 13523035 | [grwna](https://github.com/grwna) |
| Ferdinand Gabe Tua Sinaga | 13423051 | [FerdinandGabe1805](https://github.com/FerdinandGabe1805) |
| Syahrizal Bani Khairan | 13523063 | [rizalkhairan](https://github.com/rizalkhairan) |
| Daniel Pedrosa Wu | 13523099 | [DanielDPW](https://github.com/DanielDPW) |
