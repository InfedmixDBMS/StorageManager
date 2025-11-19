# Infedmix-StorageManager

## Beberapa Hal:

### Basic Storage
- Implementasi storage 1 file per table
- Block size 1024 bytes
- Buat unspanned tuple yang cross block di handle
- Write defaut ke blok terakhir, ga perlu urusin freespace, defrag berkala manual
- __special_row_id di handle di application logic (insert, update)

### Connection to other components
- Kita filtering --- query processor yang projection
- Row size ambil maks, irrespective of varchar sizes

### Metadata
- Varchar pake metadata length diawal, kalo lebih panjang dari max, di truncate
- Deleted pake deleted flag di awal setiap row
- Metadata di simpan di header, mungkin harus make variable header length
- Untuk sekarang row size tidak memperhitungkan varchar actual size sm length metadata

### Statistik

## Yang wajib untuk milestone 1
- Database udah bisa memproses request
    - Serializer beres
    - IO beres
    - read, write, delete

## Yang belum
- Indexing
- Statistic

## Pertanyaan
- Proyeksi dilakukan storage mnager query processor?
- Kalo ngeupdate varchar, handle space ny gimana?
- Klao data di fragmentasi, clustered index gimana?

## List Classes
- Serializer
- IO

## Visualization
### Row Format (example)
[ ROW HEADER ] [                         TUPLE DATA (BODY)                        ]
+------------+ +--------------------+---------------+-----------------+-----------+
| Flag | Len | |  __special_row_id  |   COL INT     |   COL Varchar   | COL FLOAT |
+------+-----+ +--------------------+---------------+-----------------+-----------+
| 1B   | 2B  | |       2 Bytes      |    4 Bytes    |     N Bytes     |  4 Bytes  |
+------+-----+ +--------------------+---------------+-----------------+-----------+