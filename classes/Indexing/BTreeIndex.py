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
    next_leaf: int
    parent_node: int
    num_keys: int
    is_leaf: bool
    is_root: bool
    keys: list[K]
    pointers: list[int | IndexPointer]

class BTreeIndex(Index[K]):
    def __init__(self, file_path: str, table: str, columns: list[str], key_type: tuple[DataType], unique: bool, **kwargs):
        super().__init__(file_path=file_path, table=table, columns=columns, key_type=key_type, unique=unique, **kwargs)
        self.root_block_index: int = 0
        self.root: BTreeNode | None = None  # Store in memory
    
    def load_metadata(self):
        self._read_index_metadata()
        self.root = self._read_node(self.root_block_index)

    # --- INTERFACE ---
    def insert(self, key: K, pointer: IndexPointer):
        # if not self.root:
        #     self.root = self._read_node(self.root_block_index)

        # node: BTreeNode = self.root
        # idx_stack: list[int] = [self.root_block_index]

        # # -------- Traverse internal nodes --------
        # while not node.is_leaf:
        #     i = 0
        #     while i < node.num_keys and key >= node.keys[i]:
        #         i += 1
        #     idx_stack.append(node.pointers[i])
        #     node = self._read_node(node.pointers[i])
        
        # # -------- Insert in leaf --------
        # def insert_key(node: BTreeIndex, entry: IndexEntry[K]):
        #     insert_pos = 0
        #     while insert_pos < node.num_keys and node.keys[insert_pos] < entry.key:
        #         insert_pos += 1
        #     node.keys.insert(insert_pos, entry.key)
        #     node.pointers.insert(insert_pos, entry.pointer)

        # # Cek unique constraint
        # if self.unique:
        #     for i in range(node.num_keys):
        #         if node.keys[i] == key:
        #             raise UniqueIndexViolationException("[BTreeIndex] Unique index violation on key {}".format(key))
        
        # # Insert key dan pointer
        # insert_key(node, IndexEntry(key=key, pointer=pointer))
        # need_write: bool = True
        # while need_write:
        #     try:
        #         self._write_through_node(idx_stack[-1], node)
        #         need_write = False
        #     except BTreeInsertedMaxKeyException:
        #         left, right, middle_entry = self._split_node(node)
        #         left_block_index: int = -1
        #         right_block_index: int = -1

        #         if len(idx_stack) == 1:
        #             left_block_index = self.io.get_last_block_index() + 1
        #             right_block_index = left_block_index + 1

        #             # Split root
        #             new_root = BTreeNode(
        #                 next_leaf=0,
        #                 parent_node=0,
        #                 num_keys=1,
        #                 is_leaf=False,
        #                 is_root=True,
        #                 keys=[],
        #                 pointers=[]
        #             )
        #             left.parent_node = self.root_block_index
        #             right.parent_node = self.root_block_index
        #             new_root.keys = [middle_entry.key]
        #             new_root.pointers = [left_block_index, right_block_index]

        #             self._write_through_node(self.root_block_index, new_root)
        #             self.root = new_root
        #             need_write = False
        #         else:
        #             left_block_index = idx_stack.pop()
        #             right_block_index = self.io.get_last_block_index() + 1

        #             # Pop parent node
        #             parent_block_index = idx_stack[-1]
        #             parent_node = self._read_node(parent_block_index)

        #             insert_key(parent_node, middle_entry)
        #             left.parent_node = parent_block_index
        #             right.parent_node = parent_block_index
        #             node = parent_node
        #             need_write = True
        #         # Write left and right nodes
        #         self._write_through_node(left_block_index, left)
        #         self._write_through_node(right_block_index, right)
        
        return

    def delete(self, key: K, specific_entry_pointer: IndexPointer | None = None) -> int:
        # TODO: implement B-Tree delete logic
        pass

    def search(self, key: K) -> Iterator[IndexEntry[K]]:
        if not self.root:
            self.root = self._read_node(self.root_block_index)

        node: BTreeNode = self.root

        # -------- Traverse internal nodes --------
        while not node.is_leaf:
            i = 0
            while i < node.num_keys and key >= node.keys[i]:
                i += 1
            node = self._read_node(node.pointers[i])

        # -------- Search in leaf --------
        leaf = node
        idx = 0
        while True:
            # Scan current leaf
            while idx < leaf.num_keys:
                k = leaf.keys[idx]
                if k > key:
                    return
                if k == key:
                    yield IndexEntry(key=k, pointer=leaf.pointers[idx])  # type: ignore

                idx += 1

            # Leaf sebelahnya jika ada
            if leaf.next_leaf == 0:
                return
            leaf = self._read_node(leaf.next_leaf)
            idx = 0

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


    # --- METADATA ---
    def _initialize_index_file(self):
        """
        Metadata index di blok 0:
            - root node block index (4 bytes)
            - jumlah kolom dalam key (2 bytes)
            - key type(s) (1 byte):
                - 'i' : int
                - 'f' : float
                - 'c' : char
                - 'v' : varchar
        """
        # --- METADATA ---
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
            pointers=[]
        )
        self._write_through_node(self.root_block_index, root_node)
    
    def _write_index_metadata(self):
        """
        Tulis metadata index ke blok 0.
        """
        metadata: list[bytes] = []
        # Root node block index
        metadata.append(struct.pack("<I", self.root_block_index))
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

        key_count = struct.unpack("<H", block[pointer : pointer + 2])[0]
        pointer += 2

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

    # --- NODE IO ---
    # Node Structure: HEADER | Key[] | Pointer[]
    # 
    # Header Structure (16 bytes):
    #       HEADER = next_leaf | parent_node | num_keys | is_leaf | is_root | padding
    #   next_leaf (4 bytes, optional): pointer ke leaf node berikutnya (hanya untuk leaf node)
    #   parent_node (4 bytes, optional): pointer ke parent node (untuk selain root node)
    #   num_keys (2 bytes): jumlah key yang ada di node
    #   is_leaf (1 byte): apakah node adalah leaf ("L") atau internal ("I")
    #   is_root (1 byte): apakah node adalah root ("R") atau bukan ("N")
    #   padding (4 bytes): reserved. Untuk align 16 bytes
    # 
    # Key Structure:
    #       Key = key_length | key_data
    #   key_length (2 bytes): panjang data key
    #   key_data : data key (variable)
    # 
    # Pointer Structure (leaf):
    #       Pointer = block_index | byte_offset
    #   block_index (4 bytes): index blok tempat data disimpan
    #   byte_offset (2 bytes): offset byte dalam blok
    # Pointer Structure (internal):
    #       Pointer = child_node_offset
    #   child_node_offset (4 bytes): block index dari child node
    def _write_through_node(self, block_index: int, node: BTreeNode):
        """
        Serialize, lalu tulis node ke blok index tertentu.
        """
        serialized_node: list[bytes] = []

        # Header
        serialized_node.append(struct.pack("<I", node.next_leaf))
        serialized_node.append(struct.pack("<I", node.parent_node))
        serialized_node.append(struct.pack("<H", node.num_keys))
        serialized_node.append(struct.pack("<B", ord('L') if node.is_leaf else ord('I')))
        serialized_node.append(struct.pack("<B", ord('R') if node.is_root else ord('N')))
        serialized_node.append(b'\x00' * 4)  # padding

        # Keys
        for key in node.keys:
            for i, key_type in enumerate(self.key_types):
                try:
                    key_type.validate(key[i])
                except Exception as e:
                    raise ValueError("[StorageManager] Key value type mismatch during serialization. " + str(e))
                if isinstance(key_type, IntType):
                    serialized_node.append(struct.pack("<H", 4))
                    serialized_node.append(struct.pack("<I", key[i]))
                elif isinstance(key_type, FloatType):
                    serialized_node.append(struct.pack("<H", 4))
                    serialized_node.append(struct.pack("<f", key[i]))
                elif isinstance(key_type, CharType) or isinstance(key_type, VarCharType):
                    serialized_node.append(struct.pack("<H", len(key[i])))
                    serialized_node.append(key[i])
                else:
                    raise ValueError("[StorageManager] Unknown key type")

        # Pointers
        for pointer in node.pointers:
            if node.is_leaf:
                # Leaf pointer
                serialized_node.append(struct.pack("<I", pointer.block_idx))
                serialized_node.append(struct.pack("<H", pointer.offset))
            else:
                # Internal pointer
                serialized_node.append(struct.pack("<I", pointer))

        block = b"".join(serialized_node)
        if len(block) > BLOCK_SIZE:
            raise BTreeInsertedMaxKeyException("[BTreeIndex] Index metadata exceeds block size")

        self.io.write(block_index, block)

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
        pointer += 4  # padding

        keys: list[K] = []
        for _ in range(num_keys):
            key_parts: list = []
            for key_type in self.key_types:
                key_length = struct.unpack("<H", block[pointer : pointer + 2])[0]
                pointer += 2
                if isinstance(key_type, IntType):
                    key_value = struct.unpack("<I", block[pointer : pointer + 4])[0]
                    pointer += 4
                elif isinstance(key_type, FloatType):
                    key_value = struct.unpack("<f", block[pointer : pointer + 4])[0]
                    pointer += 4
                elif isinstance(key_type, CharType) or isinstance(key_type, VarCharType):
                    key_value = block[pointer : pointer + key_length]
                    pointer += key_length
                else:
                    raise ValueError("[StorageManager] Unknown key type during deserialization")
                key_parts.append(key_value)
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
            pointers=pointers
        )
    
    # --- NODE OPERATIONS ---
    def _split_node(self, node: BTreeNode) -> tuple[BTreeNode, BTreeNode, IndexEntry[K]]:
        """
        Split node menjadi dua node baru.
        Mengembalikan tuple (left_node, right_node, middle_entry).
        """
        mid_index = node.num_keys // 2
        middle_entry = IndexEntry(key=node.keys[mid_index], pointer=node.pointers[mid_index])

        left_node = BTreeNode(
            next_leaf=0,
            parent_node=node.parent_node,
            num_keys=mid_index,
            is_leaf=node.is_leaf,
            is_root=False,
            keys=node.keys[:mid_index],
            pointers=node.pointers[:mid_index + (1 if not node.is_leaf else 0)]
        )

        right_node = BTreeNode(
            next_leaf=0,
            parent_node=node.parent_node,
            num_keys=node.num_keys - mid_index - 1,
            is_leaf=node.is_leaf,
            is_root=False,
            keys=node.keys[mid_index + 1:],
            pointers=node.pointers[mid_index + 1 + (1 if not node.is_leaf else 0):]
        )

        if node.is_leaf:
            left_node.next_leaf = self.io.get_last_block_index() + 2
            right_node.next_leaf = node.next_leaf

        return left_node, right_node, middle_entry

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
            node = self._read_node(node.pointers[0])
        # -------- Scan leaf nodes --------
        leaf = node
        idx = 0
        while True:
            while idx < leaf.num_keys:
                yield IndexEntry(key=leaf.keys[idx], pointer=leaf.pointers[idx])
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
            i = 0
            while i < node.num_keys and key_part > node.keys[i][0]:
                i += 1
            node = self._read_node(node.pointers[i])

        # -------- Scan leaf nodes --------
        leaf = node
        idx = 0
        while idx < leaf.num_keys and leaf.keys[idx][0] < key_part:
            idx += 1

        while True:
            while idx < leaf.num_keys:
                yield IndexEntry(key=leaf.keys[idx], pointer=leaf.pointers[idx])
                idx += 1

            # Traverse leaf sebelahnya jika ada
            if leaf.next_leaf == 0:
                return
            leaf = self._read_node(leaf.next_leaf)
            idx = 0