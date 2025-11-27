import struct
from typing import Iterator, TypeVar
from dataclasses import dataclass
from classes.DataModels import Condition, Operation, OPERATION_FUNCS
from classes.Types import DataType, IntType, FloatType, CharType, VarCharType
from classes.globals import BLOCK_SIZE
from classes.Indexing.Index import Index, IndexPointer, IndexEntry, UniqueIndexViolationException

ROOT_BLOCK_INDEX = 1  # blok index untuk root node B-Tree

K = TypeVar("K", bound=tuple)  # Index Key type

class BTreeInsertedMaxKeyException(Exception):
    pass

@dataclass
class BTreeNode:
    """
    Struktur node dalam B-Tree
    Untuk tree yang tidak unique, diperlukan diskriminator tambahan pada key.
    Untuk leaf node, diskriminator adalah IndexPointer.
    Untuk internal node, diskriminator akan diappend ke key untuk pencarian.
    Selalu gunakan method get_key() untuk mendapatkan key.
    """
    next_leaf: int
    parent_node: int
    num_keys: int
    is_leaf: bool
    is_root: bool
    keys: list[K]
    pointers: list[int | IndexPointer]
    height: int = 0
    is_unique: bool = True

    def get_key(self, index: int) -> K:
        """
        Mengembalikan key pada index tertentu, beserta diskriminator jika ada
        """
        if not self.is_unique and self.is_leaf:
            return self.keys[index] + (self.pointers[index].block_idx, self.pointers[index].offset)
        return self.keys[index]

    def get_pointer(self, index: int) -> int | IndexPointer:
        return self.pointers[index]
    
    def get_pure_key(self, index: int) -> K:
        """
        Mengembalikan key pada index tertentu, tanpa diskriminator
        """
        if not self.is_unique and not self.is_leaf:
            return self.keys[index][:-2]
        return self.keys[index]

    def search_key(self, key: K, disambiguator: IndexPointer | None = None) -> int | IndexPointer:
        """
            Untuk leaf node, mengembalikan IndexPointer untuk key yang sama
            Untuk internal node, cari int pointer ke node yang memiliki key
            Melakukan binary search pada key untuk mencari key yang sesuai
        """
        if disambiguator is not None:
            # Shorter tuple is less than longer tuple if all preceding elements are equal
            key = key + (disambiguator.block_idx, disambiguator.offset)

        # self.get_key(i) <= key < self.get_key(j)
        # with i+1 == j
        if key < self.get_key(0):
            return self.get_pointer(0)
        if key >= self.get_key(self.num_keys - 1):
            return self.get_pointer(self.num_keys)

        i = 0
        j = self.num_keys - 1
        while i + 1 < j:
            left_key = self.get_key(i)
            if left_key == key:
                j = i + 1
                break
            right_key = self.get_key(j)
            if right_key == key:
                i = j - 1
                break
            
            mid = (i + j) // 2
            mid_key = self.get_key(mid)
            if mid_key == key:
                i = mid
                j = mid + 1
                break
            elif mid_key < key:
                i = mid + 1
            else:
                j = mid - 1
        return self.get_pointer(j)

    def insert_key(self, entry: IndexEntry[K], pointer: int | IndexPointer) -> bool:
        key = entry.key
        if not self.is_unique:
            key = key + (entry.pointer.block_idx, entry.pointer.offset)
        insert_pos = 0
        while insert_pos < self.num_keys and self.get_key(insert_pos) < key:
            insert_pos += 1
        if self.is_unique and insert_pos < self.num_keys and self.get_key(insert_pos) == key:
            return False  # unique violation
        self.keys.insert(insert_pos, entry.key)
        self.pointers.insert(insert_pos + 1, pointer)
        self.num_keys += 1
        return True

    def delete_key(self, entry: IndexEntry[K]) -> bool:
        delete_idx: int = -1
        for i in range(self.num_keys):
            if self.get_pure_key(i) == entry.key:
                if self.is_unique:  # Gaperlu cek pointer (block_idx dan offset)
                    delete_idx = i
                    break
                else:
                    ptr = self.get_pointer(i)
                    if isinstance(ptr, IndexPointer) and ptr == entry.pointer:
                        delete_idx = i
                        break
        if delete_idx == -1:
            return False
        
        self.keys.pop(delete_idx)
        self.pointers.pop(delete_idx if not self.is_leaf else delete_idx)
        self.num_keys -= 1
        return True

    def split(self) -> tuple["BTreeNode", IndexEntry[K], "BTreeNode"]:
        """
        Split this node into two nodes.
        Returns a tuple: (left_node, middle_key, right_node)
        """
        mid = self.num_keys // 2
        middle_key = IndexEntry(key=self.get_key(mid), pointer=self.get_pointer(mid) if self.is_leaf else None)

        if self.is_leaf:
            # Leaf node: middle key stays in the right node
            left_keys = self.keys[:mid]
            right_keys = self.keys[mid:]

            left_ptrs = self.pointers[:mid]
            right_ptrs = self.pointers[mid:]
        else:
            # Internal node: middle key is promoted
            left_keys = self.keys[:mid]
            right_keys = self.keys[mid+1:]

            left_ptrs = self.pointers[:mid+1]
            right_ptrs = self.pointers[mid+1:]

        left_node = BTreeNode(
            is_root=False,
            is_leaf=self.is_leaf,
            parent_node=self.parent_node,
            keys=left_keys,
            pointers=left_ptrs,
            num_keys=len(left_keys),
            next_leaf=0 if not self.is_leaf else self.next_leaf,
            height=self.height,
            is_unique=self.is_unique
        )

        right_node = BTreeNode(
            is_root=False,
            is_leaf=self.is_leaf,
            parent_node=self.parent_node,
            keys=right_keys,
            pointers=right_ptrs,
            num_keys=len(right_keys),
            next_leaf=self.next_leaf if self.is_leaf else 0,
            height=self.height,
            is_unique=self.is_unique
        )

        return left_node, middle_key, right_node

class BTreeIndex(Index[K]):
    def __init__(self, index_name: str, file_path: str, table: str, columns: list[str], key_type: tuple[DataType], unique: bool, **kwargs):
        super().__init__(index_name=index_name, file_path=file_path, table=table, columns=columns, key_type=key_type, unique=unique, **kwargs)
        self.root_block_index: int = 0
        self.root: BTreeNode | None = None  # Store in memory
    
    def load_metadata(self):
        self._read_index_metadata()
        self.root = self._read_node(self.root_block_index)

    # --- INTERFACE ---
    def insert(self, entry: IndexEntry[K]) -> bool:
        if not self.root:
            self.root = self._read_node(self.root_block_index)

        node: BTreeNode = self.root
        idx_stack: list[int] = [self.root_block_index]
        nodes_stack: list[BTreeNode] = [node]
        
        # -------- Traverse internal nodes --------
        while not nodes_stack[-1].is_leaf:
            next_idx = nodes_stack[-1].search_key(entry.key, entry.pointer if not self.unique else None)
            idx_stack.append(next_idx)
            nodes_stack.append(self._read_node(next_idx))
        
        # -------- Leaf --------
        node = nodes_stack[-1]
        if not node.insert_key(entry, entry.pointer):   # Unique violation
            raise UniqueIndexViolationException(f"Unique index violation on table {self.table} for key {entry.key}")

        need_write: bool = True
        while nodes_stack and need_write:
            overflow_key: bool = False
            try:
                self._write_through_node(idx_stack[-1], nodes_stack[-1])
            except BTreeInsertedMaxKeyException:
                overflow_key = True
            if not overflow_key:
                break
            
            left_block_idx: int = -1
            right_block_idx: int = -1
            
            left_node, middle_entry, right_node = nodes_stack[-1].split()
            if len(nodes_stack) == 1:
                # Split root node. Height tree bertambah
                left_block_idx = self.io.get_last_block_index() + 1
                right_block_idx = left_block_idx + 1

                # Internal nodes technically have a link to its sibling
                left_node.next_leaf = right_block_idx
                right_node.next_leaf = 0
                left_node.parent_node = self.root_block_index
                right_node.parent_node = self.root_block_index

                root_node = BTreeNode(
                    next_leaf=0,
                    parent_node=0,
                    num_keys=1,
                    is_leaf=False,
                    is_root=True,
                    keys=[middle_entry.key],
                    pointers=[left_block_idx, right_block_idx],
                    height=nodes_stack[-1].height + 1,
                    is_unique=self.unique
                )
                if not self.unique and right_node.is_leaf: 
                    # First height increase. Update root key with disctiminator
                    right_pointer = right_node.get_pointer(0)
                    root_node.keys[0] = root_node.keys[0] + (right_pointer.block_idx, right_pointer.offset)
                self._write_through_node(self.root_block_index, root_node)
                self.root = root_node
                need_write = False
            else:
                left_block_idx = idx_stack[-1]
                right_block_idx = self.io.get_last_block_index() + 1
                
                left_node.next_leaf = right_block_idx
                right_node.next_leaf = nodes_stack[-1].next_leaf
                left_node.parent_node = nodes_stack[-1].parent_node
                right_node.parent_node = nodes_stack[-1].parent_node

                nodes_stack.pop()
                idx_stack.pop()

                nodes_stack[-1].insert_key(middle_entry, right_block_idx)  # Insert to internal node should never fail ok
                need_write = True

            try:
                self._write_through_node(left_block_idx, left_node)
                self._write_through_node(right_block_idx, right_node)
            except Exception as e:
                print(left_node.num_keys, right_node.num_keys)
                print(f"Error writing nodes: {e}")
                return False

        return True

    def delete(self, entry: IndexEntry[K]) -> bool:
        if not self.root:
            self.root = self._read_node(self.root_block_index)

        node: BTreeNode = self.root
        idx_stack: list[int] = [self.root_block_index]
        nodes_stack: list[BTreeNode] = [node]
        
        # -------- Traverse internal nodes --------
        while not nodes_stack[-1].is_leaf:
            next_idx = nodes_stack[-1].search_key(entry.key, entry.pointer if not self.unique else None)
            idx_stack.append(next_idx)
            nodes_stack.append(self._read_node(next_idx))
        
        # -------- Leaf --------
        node = nodes_stack[-1]
        success = node.delete_key(entry)
        # TODO: rebalancing nodes if underflow

        if not success:
            return False
        self._write_through_node(idx_stack[-1], nodes_stack[-1])
        return True

    def search(self, key: K) -> Iterator[IndexEntry[K]]:
        if not self.root:
            self.root = self._read_node(self.root_block_index)

        for entry in self._search_then_scan_to_end(key):
            if entry.key == key:
                yield entry
            else:
                break

    def search_condition(self, condition: Condition) -> Iterator[IndexEntry[K]]:
        col_idx = self.columns.index(condition.column)

        if col_idx != 0 or condition.operation == Operation.NEQ:
            # Ekivalen dengan full scan search
            for entry in self._full_scan():
                if OPERATION_FUNCS[condition.operation](entry.key[col_idx], condition.operand):
                    yield entry
            return

        op = condition.operation
        operand = condition.operand

        # EQ ----------------------------------------------------------------------
        if op == Operation.EQ:
            for entry in self._search_then_scan_to_end((operand,)):
                if entry.key[0] != operand:
                    break
                yield entry
            return

        # GT / GTE ----------------------------------------------------------------
        if op in (Operation.GT, Operation.GTE):
            for entry in self._search_then_scan_to_end((operand,)):
                if OPERATION_FUNCS[op](entry.key[0], operand):
                    yield entry
            return

        # LT / LTE ----------------------------------------------------------------
        if op in (Operation.LT, Operation.LTE):
            for entry in self._full_scan():   # sorted leaf-chain scan
                if OPERATION_FUNCS[op](entry.key[0], operand):
                    yield entry
                else:
                    break
            return

    # --- NODE IO ---
    """
        Node Structure: HEADER | Key[] | Pointer[]
        
        Header Structure (20 bytes):
            HEADER = next_leaf | parent_node | num_keys | is_leaf | is_root | height | next_block | padding
        next_leaf (4 bytes, optional): pointer ke leaf node berikutnya (hanya untuk leaf node)
        parent_node (4 bytes, optional): pointer ke parent node (untuk selain root node)
        num_keys (2 bytes): jumlah key yang ada di node
        is_leaf (1 byte): apakah node adalah leaf ("L") atau internal ("I")
        is_root (1 byte): apakah node adalah root ("R") atau bukan ("N")
        height (1 byte): tinggi node dalam tree (0 untuk leaf)
        padding (3 bytes): reserved. Untuk align 4 bytes
        next_block (4 byte): untuk spanning node, blok selanjutnya yang dipakai, bernilai 0 jika bukan spanning node
        
        Key Structure:
            Key = key_data
        key_data : data key
            - Untuk IntType, FloatType: 4 bytes
            - Untuk CharType, 2+n bytes (malas handle UTF-8, jadi tambhakan info panjang aktual)
            - Untuk VarCharType, 2+m bytes (dengan 2 bytes untuk length prefix)
        
        Pointer Structure (internal):
            Pointer = child_node_offset
        child_node_offset (4 bytes): block index dari child node
        Pointer Structure (leaf, unique):
            Pointer = block_index | byte_offset
        block_index (4 bytes): index blok tempat data disimpan
        byte_offset (2 bytes): offset byte dalam blok
        Pointer Structure (leaf, non-unique):
            # NOTE: If non-unique, the block index and offset are stored in the key itself to prevent redundancy
        
        Continuation block for spanning node: 
            next_block | clipped_content
        next_block (4 bytes): block index dari blok selanjutnya. 0 jika tidak ada lagi blok.
        clipped_content (max BLOCK_SIZE - 4 bytes): sisa data node yang tidak muat di blok sebelumnya.
    """
    def _write_through_node(self, block_index: int, node: BTreeNode):
        """
        Serialize, lalu tulis node ke blok index tertentu.
        """
        headers: list[bytes] = [b"" for _ in range(8)]
        serialized_node: list[bytes] = []

        # Header
        headers[0] = struct.pack("<I", node.next_leaf)
        headers[1] = struct.pack("<I", node.parent_node)
        headers[2] = struct.pack("<H", node.num_keys)
        headers[3] = struct.pack("<B", ord('L') if node.is_leaf else ord('I'))
        headers[4] = struct.pack("<B", ord('R') if node.is_root else ord('N'))
        headers[5] = struct.pack("<B", node.height + ord('0'))
        headers[6] = b'\x00' * 3  # padding
        headers[7] = struct.pack("<I", 0)  # next block for spanning node

        # Keys
        for key_idx in range(node.num_keys):
            key = node.get_pure_key(key_idx)
            for i, key_type in enumerate(self.key_types):
                try:
                    key_type.validate(key[i])
                except Exception as e:
                    raise ValueError("[StorageManager] Key value type mismatch during serialization. " + str(e))
                if isinstance(key_type, IntType):
                    serialized_node.append(struct.pack("<i", key[i]))
                elif isinstance(key_type, FloatType):
                    serialized_node.append(struct.pack("<f", key[i]))
                elif isinstance(key_type, CharType):
                    encoded = key[i].encode('utf-8')
                    padded = encoded.ljust(key_type.length, b'\x00')
                    serialized_node.append(struct.pack("<H", len(encoded)))
                    serialized_node.append(padded)
                elif isinstance(key_type, VarCharType):
                    encoded = key[i].encode('utf-8')
                    serialized_node.append(struct.pack("<H", len(encoded)))
                    serialized_node.append(encoded)
                else:
                    raise ValueError("[StorageManager] Unknown key type")
            
            if not node.is_leaf and not self.unique:
                # Append discriminator to key for internal non-unique nodes
                discriminated_key = node.get_key(key_idx)
                serialized_node.append(struct.pack("<I", discriminated_key[-2]))  # block_idx
                serialized_node.append(struct.pack("<H", discriminated_key[-1]))  # offset

        # Pointers
        if not node.is_leaf:
            for pointer in node.pointers:
                serialized_node.append(struct.pack("<I", pointer))
        elif node.is_leaf and self.unique:
            for pointer in node.pointers:
                serialized_node.append(struct.pack("<I", pointer.block_idx))
                serialized_node.append(struct.pack("<H", pointer.offset))

        blob = b"".join(headers + serialized_node)
        if len(blob) > BLOCK_SIZE and node.num_keys > 1:
            raise BTreeInsertedMaxKeyException("[BTreeIndex] Index node data exceeds block size")
        elif len(blob) > BLOCK_SIZE: # Allow 1 key node to overflow. Do not index VARCHAR guys
            # --- Spanning node ---
            # Allocate at least 1 continuation block
            new_block_idx = self.io.get_last_block_index() + 1
            headers[7] = struct.pack("<I", new_block_idx)  # next block for spanning node
            blob = b"".join(headers + serialized_node)

            blocks = [blob[0 : BLOCK_SIZE]]
            sections = []
            block_indices = [block_index]
            pointer = BLOCK_SIZE
            while pointer < len(blob):
                next_block_idx = self.io.get_last_block_index() + len(block_indices)
                new_section = blob[pointer : pointer + BLOCK_SIZE - 4]

                sections.append(new_section)
                block_indices.append(next_block_idx)
                pointer += BLOCK_SIZE - 4

            block_indices.append(0)
            for i, section in enumerate(sections):
                blocks.append(struct.pack("<I", block_indices[i + 2]) + section)
            for i, block in enumerate(blocks):
                self.io.write(block_indices[i], block)
        else:
            self.io.write(block_index, blob)

    def _read_node(self, block_index: int) -> BTreeNode:
        """
        Baca dan deserialize node dari blok index tertentu.
        """
        block = self.io.read(block_index)
        pointer = 0

        next_leaf = struct.unpack("<I", block[pointer : pointer + 4])[0]
        pointer += 4
        parent_node = struct.unpack("<I", block[pointer : pointer + 4])[0]
        pointer += 4
        num_keys = struct.unpack("<H", block[pointer : pointer + 2])[0]
        pointer += 2
        is_leaf = struct.unpack("<B", block[pointer : pointer + 1])[0] == ord('L')
        pointer += 1
        is_root = struct.unpack("<B", block[pointer : pointer + 1])[0] == ord('R')
        pointer += 1
        height = struct.unpack("<B", block[pointer : pointer + 1])[0] - ord('0')
        pointer += 1
        pointer += 3  # padding
        next_block = struct.unpack("<I", block[pointer : pointer + 4])[0]
        pointer += 4

        if next_block != 0:
            # --- Spanning node ---
            # Do not index VARCHAR guys
            sections = []
            current_block_idx = next_block
            while current_block_idx != 0:
                next_blob = self.io.read(current_block_idx)
                sections.append(next_blob)
                current_block_idx = struct.unpack("<I", next_blob[0:4])[0]
            block = block + b"".join(b[4:] for b in sections)
            pointer = 20  # after header

        keys: list[K] = []
        for _ in range(num_keys):
            key_parts: list = []
            for key_type in self.key_types:
                if isinstance(key_type, IntType):
                    key_value = struct.unpack("<i", block[pointer : pointer + 4])[0]
                    pointer += 4
                elif isinstance(key_type, FloatType):
                    key_value = struct.unpack("<f", block[pointer : pointer + 4])[0]
                    pointer += 4
                elif isinstance(key_type, CharType):
                    length = key_type.length
                    key_value = block[pointer : pointer + length].decode("utf-8").rstrip('\x00')
                    pointer += length
                elif isinstance(key_type, VarCharType):
                    length = struct.unpack("<H", block[pointer : pointer + 2])[0]
                    pointer += 2
                    key_value = block[pointer : pointer + length].decode("utf-8").rstrip('\x00')
                    pointer += length
                else:
                    raise ValueError("[StorageManager] Unknown key type during deserialization")
                key_parts.append(key_value)
            
            if not is_leaf and not self.unique:
                # Read discriminator for internal non-unique nodes
                block_idx = struct.unpack("<I", block[pointer : pointer + 4])[0]
                pointer += 4
                offset = struct.unpack("<H", block[pointer : pointer + 2])[0]
                pointer += 2
                key_parts.append(block_idx)
                key_parts.append(offset)
            keys.append(tuple(key_parts))  # type: ignore

        pointers: list[int | IndexPointer] = []
        for _ in range(num_keys + (0 if is_leaf else 1)):
            if is_leaf:
                block_idx = struct.unpack("<I", block[pointer : pointer + 4])[0]
                pointer += 4
                offset = struct.unpack("<H", block[pointer : pointer + 2])[0]
                pointer += 2
                pointers.append(IndexPointer(block_idx=block_idx, offset=offset))
            else:
                child_node_offset = struct.unpack("<I", block[pointer : pointer + 4])[0]
                pointer += 4
                pointers.append(child_node_offset)
        return BTreeNode(
            next_leaf=next_leaf,
            parent_node=parent_node,
            num_keys=num_keys,
            is_leaf=is_leaf,
            is_root=is_root,
            keys=keys,
            pointers=pointers,
            height=height,
            is_unique=self.unique
        )

    # --- Traversal algorithms ---
    def _full_scan(self) -> Iterator[IndexEntry[K]]:
        """
        Scan seluruh entry index dari awal sampai akhir.
        """
        if not self.root:
            self.root = self._read_node(self.root_block_index)

        node = self.root

        # -------- Traverse internal nodes --------
        # Traverse leaf terkiri paling bawah
        while not node.is_leaf:
            node = self._read_node(node.get_pointer(0))
        # -------- Scan leaf nodes --------
        leaf = node
        idx = 0
        while True:
            while idx < leaf.num_keys:
                yield IndexEntry(key=leaf.get_pure_key(idx), pointer=leaf.get_pointer(idx))
                idx += 1

            # Traverse leaf sebelahnya jika ada
            if leaf.next_leaf == 0:
                break
            leaf = self._read_node(leaf.next_leaf)
            idx = 0
    
    def _search_then_scan_to_end(self, key: tuple) -> Iterator[IndexEntry[K]]:
        """
        Scan semua entry yang komponen pertama keynya lebih besar dari key yang diberikan.
        Jika komposit, hanya komponen pertama yang dibandingkan. Komponen kedua diabaikan.
        Tidak dapat melakukan searching ke komponen selain komponen pertama.
        """
        
        if not self.root:
            self.root = self._read_node(self.root_block_index)

        key_part = key[0]
        node = self.root

        # -------- Traverse internal nodes --------
        while not node.is_leaf:
            child_node_idx = node.search_key((key_part,))
            node = self._read_node(child_node_idx)

        # -------- Scan leaf nodes --------
        leaf = node
        idx = 0
        while idx < leaf.num_keys and leaf.get_pure_key(idx)[0] < key_part:
            idx += 1

        while True:
            while idx < leaf.num_keys:
                yield IndexEntry(key=leaf.get_pure_key(idx), pointer=leaf.get_pointer(idx))
                idx += 1

            # Traverse leaf sebelahnya jika ada
            if leaf.next_leaf == 0:
                return
            leaf = self._read_node(leaf.next_leaf)
            idx = 0
    
    # --- METADATA ---
    """
    Metadata index di blok 0:
        - root node block index (4 bytes)
        - unique flag (1 byte) ['U', 'D']
        - jumlah kolom dalam key (2 bytes)
        - key type(s) (1 byte):
            - 'i' : int
            - 'f' : float
            - 'c' : char
            - 'v' : varchar
    """
    def _initialize_index_file(self):
        self.root_block_index = ROOT_BLOCK_INDEX
        self._write_index_metadata()

        # --- ROOT NODE ---
        root_node = BTreeNode(
            next_leaf=0,
            parent_node=0,
            num_keys=0,
            is_leaf=True,
            is_root=True,
            keys=[],
            pointers=[],
            height=0,
            is_unique=self.unique
        )
        self._write_through_node(self.root_block_index, root_node)
    
    def _write_index_metadata(self):
        """
        Tulis metadata index ke blok 0.
        """
        metadata: list[bytes] = []
        # Root node block index
        metadata.append(struct.pack("<I", self.root_block_index))
        # Unique flag
        metadata.append(struct.pack("<B", ord('U') if self.unique else ord('D')))
        # Jumlah kolom dalam key
        metadata.append(struct.pack("<H", len(self.key_types)))
        # Key types
        for typ in self.key_types:
            if isinstance(typ, IntType):
                metadata.append(struct.pack("<B", ord('i')))
            elif isinstance(typ, FloatType):
                metadata.append(struct.pack("<B", ord('f')))
            elif isinstance(typ, CharType):
                metadata.append(struct.pack("<B", ord('c')))
            elif isinstance(typ, VarCharType):
                metadata.append(struct.pack("<B", ord('v')))

        self.io.write(0, b"".join(metadata))

    def _read_index_metadata(self):
        """
        Membaca metadata index dari blok 0.
        """
        block = self.io.read(0)
        pointer = 0
        self.root_block_index = struct.unpack("<I", block[pointer : pointer + 4])[0]
        pointer += 4
        unique_flag = struct.unpack("<B", block[pointer : pointer + 1])[0]
        unique = unique_flag == ord('U')
        pointer += 1
        key_count = struct.unpack("<H", block[pointer : pointer + 2])[0]
        pointer += 2

        if unique != self.unique:
            raise ValueError("[StorageManager] Unique flag in metadata does not match initialized unique flag")

        # Key type validation
        types = []
        for _ in range(key_count):
            key_type = struct.unpack("<B", block[pointer : pointer + 1])[0]
            types.append(key_type)
            pointer += 1

        if len(types) != len(self.key_types):
            raise ValueError("[StorageManager] Key types count in metadata does not match initialized key types count")
        for i, t in enumerate(types):
            if t == ord('i') and not isinstance(self.key_types[i], IntType):
                raise ValueError("[StorageManager] Key type mismatch for key column {}".format(i))
            elif t == ord('f') and not isinstance(self.key_types[i], FloatType):
                raise ValueError("[StorageManager] Key type mismatch for key column {}".format(i))
            elif t == ord('c') and not isinstance(self.key_types[i], CharType):
                raise ValueError("[StorageManager] Key type mismatch for key column {}".format(i))
            elif t == ord('v') and not isinstance(self.key_types[i], VarCharType):
                raise ValueError("[StorageManager] Key type mismatch for key column {}".format(i))
