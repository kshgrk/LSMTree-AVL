This is a Python implementation of an LSM-tree database using AVL trees for both the memtable and index.

## Roadmap

- [x]  **AVL Tree Implementation:**
   - Implement a balanced AVL tree data structure for efficient key-value storage and retrieval.

- [x]  **Bloom Filter Implementation:**
   - Implement a Bloom filter to quickly check if a key might be present in an SSTable before performing a full search.

- [ ]  **LSMTree:**
   - [x] **Memtable (AVL):** Use an AVL tree as the in-memory memtable to store recent writes.
   - [x] **Index (AVL):** Use an AVL tree as the index to efficiently locate keys within SSTables.
      - [ ] Will store min key for each segment.
   - [ ] **Compaction:** Implement strategies for merging and compacting SSTables to reduce redundancy and improve read performance.
      - [ ] On separate thread for uninteruppted reads and writes. 
   - [x] **Memtable Flush:** Implement the process of flushing the memtable to disk as an SSTable when it crosses treshold.
   - [x] **Write Ahead Log:** For saving data in-case of crashes or failure.
