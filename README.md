# Infedmix-StorageManager

## Beberapa Hal:
- Implementasi storage 1 file per table
- Block size 1024 bytes
- Varchar disimpan sebagai fixed length data (for now)
- Buat unspanned tuple yang cross block di handle
- Kita filtering --- query processor yang projection
- Varchar pake metadata length diawal, kalo lebih panjang dari max, di truncate
- Row size ambil maks, irrespective of varchar sizes

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