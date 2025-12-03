import struct
import math
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
    Key pada index yang tidak unique akan diappend discriminator dalam pentimpanan
    Gunakan get_pure_key() untuk mendapatkan key tanpa diskriminator
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

    def get_num_keys(self) -> int:
        return self.num_keys

    def get_key(self, index: int) -> K:
        """
        Mengembalikan key pada index tertentu, beserta diskriminator jika ada
        """
        return self.keys[index]

    def get_pointer(self, index: int) -> int | IndexPointer:
        if self.is_leaf and not self.is_unique:
            discriminator = self.keys[index][-2:]  # Get discriminator from key
            pointer = IndexPointer(block_idx=discriminator[0], offset=discriminator[1])
            return pointer
        return self.pointers[index]
    
    def get_pure_key(self, index: int) -> K:
        """
        Mengembalikan key pada index tertentu, tanpa diskriminator
        """
        if not self.is_unique:
            return self.keys[index][:-2]  # Remove discriminator from key
        return self.keys[index]

    def search_key(self, key: K, disambiguator: IndexPointer | None = None) -> int | IndexPointer:
        """
            Untuk leaf node, mengembalikan IndexPointer untuk key yang sama
            Untuk internal node, cari int pointer ke node yang memiliki key
            Melakukan binary search pada key untuk mencari key yang sesuai
        """
        if disambiguator is not None:
            key = key + (disambiguator.block_idx, disambiguator.offset)

        if len(self.keys) == 0:
            raise RuntimeError("BTreeNode has no keys to search.")

        # self.get_key(i) <= key < self.get_key(j)
        # with i+1 == j
        if key < self.get_key(0):
            return self.get_pointer(0)
        if key >= self.get_key(self.get_num_keys() - 1):
            return self.get_pointer(self.get_num_keys())

        i = 0
        j = self.get_num_keys() - 1
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
        
        # After binary search: find largest index where keys[idx] <= key, then return pointer[idx+1]
        # The loop maintains keys[i] <= key < keys[j] but may exit with i+1 == j or i+1 < j
        if key >= self.get_key(j):
            return self.get_pointer(j + 1)
        elif key >= self.get_key(i):
            return self.get_pointer(i + 1)
        else:
            # Shouldn't happen given boundary checks, but return pointer[i] as fallback
            return self.get_pointer(i)

    def insert_key(self, entry: IndexEntry[K], pointer: int | IndexPointer) -> bool:
        storage_key = entry.key

        if not self.is_unique and isinstance(pointer, IndexPointer):
            # Store pointer with the key
            storage_key = entry.key + (pointer.block_idx, pointer.offset)
        
        insert_pos = 0
        while insert_pos < self.get_num_keys() and self.get_key(insert_pos) < storage_key:
            insert_pos += 1
        
        # Check unique violation
        if self.is_unique and insert_pos < self.get_num_keys() and self.get_key(insert_pos) == storage_key:
            return False
        
        self.keys.insert(insert_pos, storage_key)
        self.pointers.insert(insert_pos + 1, pointer)
        self.num_keys += 1
        return True

    def delete_key(self, entry: IndexEntry[K]) -> bool:
        delete_idx: int = -1
        for i in range(self.get_num_keys()):
            if self.get_pure_key(i) == entry.key:
                if self.is_unique:  # Gaperlu cek pointer (block_idx dan offset)
                    delete_idx = i
                    break
                elif self.get_key(i) == entry.key + (entry.pointer.block_idx, entry.pointer.offset):
                    delete_idx = i
                    break
        if delete_idx == -1:
            return False
        
        self.keys.pop(delete_idx)
        self.pointers.pop(delete_idx)
        self.num_keys -= 1
        return True

    def split(self) -> tuple["BTreeNode", IndexEntry[K], "BTreeNode"]:
        """
        Returns a tuple: (left_node, middle_key, right_node)
        """
        mid = math.ceil((self.get_num_keys() - 1) / 2)  # Right-biased split
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
        
        # Calculate conservative order based on worst-case key sizes
        self.order: int = self._calculate_max_order()
        self.min_keys: int = math.ceil(self.order / 2) - 1
    
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

        # Check overflow using order
        need_write: bool = True
        while nodes_stack and need_write:
            # Check if node exceeds order threshol before writing
            overflow_key: bool = nodes_stack[-1].num_keys >= self.order
            
            if not overflow_key:
                # Safe to write
                self._write_through_node(idx_stack[-1], nodes_stack[-1])
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

                # Pop the split child from stacks
                idx_stack.pop()
                nodes_stack.pop()
                
                parent = nodes_stack[-1]
                parent.insert_key(middle_entry, right_block_idx)
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
        node_idx = idx_stack[-1]
        success = node.delete_key(entry)

        if not success:
            return False
        
        # Check for underflow and rebalance if needed
        if node.num_keys < self.min_keys and not node.is_root:
            # Get parent for underflow handling
            parent_idx = idx_stack[-2]
            parent = nodes_stack[-2]
            self._handle_underflow(node_idx, node, parent_idx, parent)
        elif node.is_root and node.num_keys == 0 and not node.is_leaf:
            # Root empty but not leaf, promote only child as new root
            first_child_idx = node.pointers[0]
            first_child = self._read_node(first_child_idx)
            first_child.is_root = True
            first_child.parent_node = 0
            self.root = first_child
            self.root_block_index = first_child_idx
            self._write_through_node(first_child_idx, first_child)
            self._write_index_metadata()
        else:
            # No underflow, just write the node
            self._write_through_node(node_idx, node)
        
        return True

    # --- UNDERFLOW HANDLING ---
    def _update_separator_key(self, parent: BTreeNode, sep_idx: int, right_node: BTreeNode) -> None:
        """
        Update separator key in parent to correctly point to first key of right node.
        Handles discriminators for non-unique indexes.
        """
        if right_node.is_leaf and not self.unique:
            # Leaf non-unique: separator needs discriminated key
            first_ptr = right_node.get_pointer(0)
            parent.keys[sep_idx] = right_node.keys[0] + (first_ptr.block_idx, first_ptr.offset)
        elif not right_node.is_leaf and not self.unique:
            # Internal non-unique: get_key() already includes discriminator
            parent.keys[sep_idx] = right_node.get_key(0)
        else:
            # Unique (leaf or internal): just use the key
            parent.keys[sep_idx] = right_node.keys[0]

    def _handle_underflow(self, node_idx: int, node: BTreeNode, 
                          parent_idx: int, parent: BTreeNode) -> None:
        """
        Handle underflow after deletion. Tries to:
        1. Borrow from left sibling (if it has spare keys)
        2. Borrow from right sibling (if it has spare keys)
        3. Merge with left sibling
        4. Merge with right sibling
        """
        # Find node's position in parent
        child_position = -1
        for i, ptr in enumerate(parent.pointers):
            if ptr == node_idx:
                child_position = i
                break
        
        if child_position == -1:
            return  # Node not found in parent
        
        # Get siblings
        left_sibling_idx = None
        left_sibling = None
        if child_position > 0:
            left_sibling_idx = parent.pointers[child_position - 1]
            if isinstance(left_sibling_idx, int):
                left_sibling = self._read_node(left_sibling_idx)
        
        right_sibling_idx = None
        right_sibling = None
        if child_position < len(parent.pointers) - 1:
            right_sibling_idx = parent.pointers[child_position + 1]
            if isinstance(right_sibling_idx, int):
                right_sibling = self._read_node(right_sibling_idx)
        
        # Try borrowing from siblings
        if left_sibling and left_sibling.num_keys > self.min_keys:
            self._borrow_from_left(node_idx, node, left_sibling_idx, left_sibling, 
                                   parent_idx, parent, child_position)
        elif right_sibling and right_sibling.num_keys > self.min_keys:
            self._borrow_from_right(node_idx, node, right_sibling_idx, right_sibling,
                                    parent_idx, parent, child_position)
        # Try merging
        elif left_sibling:
            self._merge_nodes(left_sibling_idx, left_sibling, node_idx, node,
                             parent_idx, parent, child_position - 1)
        elif right_sibling:
            self._merge_nodes(node_idx, node, right_sibling_idx, right_sibling,
                             parent_idx, parent, child_position)

    def _borrow_from_left(self, node_idx: int, node: BTreeNode,
                          left_idx: int, left_sibling: BTreeNode,
                          parent_idx: int, parent: BTreeNode,
                          child_position: int) -> None:
        """
        Borrow the rightmost key from left sibling.
        """
        if node.is_leaf:
            # Borrow rightmost key from left sibling
            borrowed_key = left_sibling.keys.pop()
            borrowed_ptr = left_sibling.pointers.pop()
            left_sibling.num_keys -= 1
            
            # Insert at beginning of current node
            node.keys.insert(0, borrowed_key)
            node.pointers.insert(0, borrowed_ptr)
            node.num_keys += 1
            
            # Update parent separator to point to first key of right node
            self._update_separator_key(parent, child_position - 1, node)
        else:
            # Internal node: borrow key and rotate through parent
            borrowed_key = left_sibling.keys.pop()
            borrowed_ptr = left_sibling.pointers.pop()
            left_sibling.num_keys -= 1
            
            # Parent key comes down to current node
            parent_key = parent.keys[child_position - 1]
            
            # Insert parent key at beginning of current node
            node.keys.insert(0, parent_key)
            node.pointers.insert(0, borrowed_ptr)
            node.num_keys += 1
            
            # Borrowed key goes up to parent
            parent.keys[child_position - 1] = borrowed_key
            
            # Update borrowed child's parent pointer
            if isinstance(borrowed_ptr, int):
                borrowed_child = self._read_node(borrowed_ptr)
                borrowed_child.parent_node = node_idx
                self._write_through_node(borrowed_ptr, borrowed_child)
        
        # Write changes
        self._write_through_node(left_idx, left_sibling)
        self._write_through_node(node_idx, node)
        self._write_through_node(parent_idx, parent)

    def _borrow_from_right(self, node_idx: int, node: BTreeNode,
                           right_idx: int, right_sibling: BTreeNode,
                           parent_idx: int, parent: BTreeNode,
                           child_position: int) -> None:
        """
        Borrow the leftmost key from right sibling.
        """
        if node.is_leaf:
            # Borrow leftmost key from right sibling
            borrowed_key = right_sibling.keys.pop(0)
            borrowed_ptr = right_sibling.pointers.pop(0)
            right_sibling.num_keys -= 1
            
            # Append to current node
            node.keys.append(borrowed_key)
            node.pointers.append(borrowed_ptr)
            node.num_keys += 1
            
            # Update parent separator to point to new first key of right sibling
            self._update_separator_key(parent, child_position, right_sibling)
        else:
            # Internal node: borrow key and rotate through parent
            borrowed_key = right_sibling.keys.pop(0)
            borrowed_ptr = right_sibling.pointers.pop(0)
            right_sibling.num_keys -= 1
            
            # Parent key comes down to current node
            parent_key = parent.keys[child_position]
            
            # Append parent key to current node
            node.keys.append(parent_key)
            node.pointers.append(borrowed_ptr)
            node.num_keys += 1
            
            # Borrowed key goes up to parent
            parent.keys[child_position] = borrowed_key
            
            # Update borrowed child's parent pointer
            if isinstance(borrowed_ptr, int):
                borrowed_child = self._read_node(borrowed_ptr)
                borrowed_child.parent_node = node_idx
                self._write_through_node(borrowed_ptr, borrowed_child)
        
        # Write changes
        self._write_through_node(right_idx, right_sibling)
        self._write_through_node(node_idx, node)
        self._write_through_node(parent_idx, parent)

    def _merge_nodes(self, left_idx: int, left_node: BTreeNode,
                     right_idx: int, right_node: BTreeNode,
                     parent_idx: int, parent: BTreeNode,
                     separator_key_idx: int) -> None:
        """
        Merge right node into left node. Handles recursive underflow in parent.
        """
        separator_key = parent.keys[separator_key_idx]
        
        if left_node.is_leaf:
            # Leaf: just concatenate keys and pointers
            left_node.keys.extend(right_node.keys)
            left_node.pointers.extend(right_node.pointers)
            left_node.num_keys += right_node.num_keys
            
            # Update leaf chain to skip merged node
            left_node.next_leaf = right_node.next_leaf
        else:
            # Internal: separator key comes down from parent
            left_node.keys.append(separator_key)
            left_node.keys.extend(right_node.keys)
            left_node.pointers.extend(right_node.pointers)
            left_node.num_keys += right_node.num_keys + 1
            
            # Update all moved children's parent pointers
            for ptr in right_node.pointers:
                if isinstance(ptr, int):
                    child = self._read_node(ptr)
                    child.parent_node = left_idx
                    self._write_through_node(ptr, child)
        
        # Remove separator key from parent
        parent.keys.pop(separator_key_idx)
        parent.pointers.pop(separator_key_idx + 1)
        parent.num_keys -= 1
        
        # Write merged left node
        self._write_through_node(left_idx, left_node)
        
        # Check if parent underflows (recursive underflow)
        if parent.num_keys < self.min_keys and not parent.is_root:
            # Need to get parent's parent for recursive handling
            grandparent_idx = parent.parent_node
            grandparent = self._read_node(grandparent_idx)
            self._handle_underflow(parent_idx, parent, grandparent_idx, grandparent)
        elif parent.is_root and parent.num_keys == 0:
            # Root is empty, promote left child as new root
            left_node.is_root = True
            left_node.parent_node = 0
            self.root = left_node
            self.root_block_index = left_idx
            self._write_through_node(left_idx, left_node)
            # Update metadata with new root block index
            self._write_index_metadata()
        else:
            # Parent is fine, just write it
            self._write_through_node(parent_idx, parent)

    def _normalize_key(self, key: K) -> K:
        """
        Normalize key values through float32 serialization to match stored values.
        FloatType uses 4-byte floats which can't exactly represent all decimal values.
        """
        import struct
        from classes.Types import FloatType
        
        normalized = []
        for i, val in enumerate(key):
            if i < len(self.key_types) and isinstance(self.key_types[i], FloatType):
                # Round-trip through float32 to match stored precision
                normalized.append(struct.unpack('<f', struct.pack('<f', float(val)))[0])
            else:
                normalized.append(val)
        return tuple(normalized)

    def search(self, key: K) -> Iterator[IndexEntry[K]]:
        if not self.root:
            self.root = self._read_node(self.root_block_index)

        # Normalize key to match float32 precision of stored values
        normalized_key = self._normalize_key(key)
        
        for entry in self._search_then_scan_to_end(key):
            if entry.key == normalized_key:
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
            Pointer = <none>
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
                    serialized_node.append(padded)
                elif isinstance(key_type, VarCharType):
                    encoded = key[i].encode('utf-8')
                    serialized_node.append(struct.pack("<H", len(encoded)))
                    serialized_node.append(encoded)
                else:
                    raise ValueError("[StorageManager] Unknown key type")
            
            if not self.unique:
                # Append discriminator to key for non-unique trees
                discriminated_key = node.get_key(key_idx)
                serialized_node.append(struct.pack("<I", discriminated_key[-2]))  # block_idx
                serialized_node.append(struct.pack("<H", discriminated_key[-1]))  # offset

        # Pointers
        if not node.is_leaf:
            for pointer in node.pointers:
                serialized_node.append(struct.pack("<I", pointer))
        elif node.is_leaf and self.unique:
            # Non-unique leaf nodes store pointer in the key itself
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
                    key_value = block[pointer : pointer + key_type.length].decode("utf-8").rstrip('\x00')
                    pointer += key_type.length
                elif isinstance(key_type, VarCharType):
                    length = struct.unpack("<H", block[pointer : pointer + 2])[0]
                    pointer += 2
                    key_value = block[pointer : pointer + length].decode("utf-8").rstrip('\x00')
                    pointer += length
                else:
                    raise ValueError("[StorageManager] Unknown key type during deserialization")
                key_parts.append(key_value)
            
            if not self.unique:
                # Read discriminator for internal non-unique nodes
                block_idx = struct.unpack("<I", block[pointer : pointer + 4])[0]
                pointer += 4
                offset = struct.unpack("<H", block[pointer : pointer + 2])[0]
                pointer += 2
                key_parts.append(block_idx)
                key_parts.append(offset)
            keys.append(tuple(key_parts))  # type: ignore

        pointers: list[int | IndexPointer] = []
        for i in range(num_keys + (0 if is_leaf else 1)):
            if is_leaf and self.unique:
                # Both unique and non-unique leaf nodes: read pointer from disk
                block_idx = struct.unpack("<I", block[pointer : pointer + 4])[0]
                pointer += 4
                offset = struct.unpack("<H", block[pointer : pointer + 2])[0]
                pointer += 2
                pointers.append(IndexPointer(block_idx=block_idx, offset=offset))
            elif not is_leaf:
                # Internal node: read child block index
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

        # Normalize key_part to match float32 precision if needed
        key_part = self._normalize_key(key)[0]
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
    
    def _calculate_max_order(self) -> int:
        """
        Calculate conservative max order based on worst-case key sizes.
        This ensures all keys will fit when serialized, enabling predictable
        B-tree behavior with fixed order across all nodes.
        """
        # Calculate worst-case key size
        max_key_size = 0
        for key_type in self.key_types:
            if isinstance(key_type, IntType):
                max_key_size += 4  # 4 bytes for int
            elif isinstance(key_type, FloatType):
                max_key_size += 4  # 4 bytes for float
            elif isinstance(key_type, CharType):
                max_key_size += key_type.length  # max chars
            elif isinstance(key_type, VarCharType):
                max_key_size += 2 + key_type.max_length  # length prefix + max chars (worst case)
        
        # Add discriminator overhead for non-unique indexes
        if not self.unique:
            max_key_size += 6  # block_idx(4) + offset(2) discriminator
        
        # Determine worst-case space per entry
        # Internal nodes: n keys + (n+1) pointers → n×key + (n+1)×4 → per entry: key+4, plus 4 extra
        # Leaf nodes (both unique and non-unique): n keys + n pointers → n×key + n×6 → per entry: key+6
        
        # Leaf nodes are more restrictive: n×(key+6) ≤ available
        # For non-unique, keys include discriminator, so max_key_size already accounts for it
        bytes_per_entry = max_key_size + 6
        
        available_space = BLOCK_SIZE - 20
        order = (available_space - 4) // bytes_per_entry
        
        # Minimum order of 2 for B-tree validity
        return max(2, order)
    
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
