# Changelog

### Phase 1: Foundation
- Project setup, YAML metadata loader, parquet chunk reader with mmap

### Phase 2: Scanning & Filtering
- Predicate system (Eq/InList/BloomFilter/Range/And/Or), parallel row-group scanner, block range filter
- Multi-stage RowFilter cascading (most selective column first)

### Phase 3: Query Language
- JSON query parser, schema-driven validation, plan compiler
- camelCase <-> snake_case, discriminator dispatch (d1/d2/d4/d8/d3-d16)

### Phase 4: Join Engine
- Semi-join (hash-based), lookup-join, hierarchical join (instruction_address prefix matching)
- Multi-column composite keys, bidirectional (children + parents)

### Phase 5: Output Assembly
- JSON encoders (Value, BigNum, Json, SolanaTransactionVersion, TimestampSecond)
- Block grouping, roll() for a0-a15 + rest_accounts, weight-based size limits
- Streaming JSON writer with 16KB flush threshold

### Phase 6: Optimization Round 1
- RowFilter predicate pushdown, parallel relation scans, pre-computed JSON field writers
- Cross-table row group pruning, O(1) block indexing, cached ArrowReaderMetadata
- jemalloc, batch size 65536, early exit on 0 primary rows

### Phase 7: Low-Level Performance
- mmap I/O (memmap2 + Bytes::from_owner)
- Pre-built typed HashSet in InListPredicate + BooleanBufferBuilder
- Batch size usize::MAX (one batch per row group)
- faster-hex SIMD encoding, resolved field writers, typed join extractors

### Phase 8: Relation Scan Pushdown
- KeyFilter: push join keys from primary scan into relation scans as RowFilter stage
- Composite key serialization with cross-type normalization (UInt8/16/32/64, Int16/32/64 -> u64)
- Row group pruning by block number set (binary search on sorted blocks)
- solana_hard/large: 288.9ms -> 123.8ms (2.33x speedup)

### Phase 9: Final Optimizations
- Skip redundant join for Join-type relations when KeyFilter already applied
- HierarchicalFilter as RowFilter stage (children/parents filtering during scan, not post-scan)
- Bug fix: UInt16 instruction_address handling (was silently returning 0 children)
- Parallel JSON generation via rayon par_iter (47ms -> 13ms)
- TypedKeyColumn: resolve column types once per batch, fixed-size stack buffer for 2-column keys

### E2E Tests & Sort Optimization
- 46 fixture-based e2e tests comparing output against legacy engine (all passing)
- Relation scoping via `source_predicates` (per-item relation filtering, not global)
- Fixed `item_order_keys` for statediffs, balances, token_balances, rewards
- Field group emission: emit group when writers exist, not based on non-null values
- **Sort precompute**: `build_full_sort_columns()` + column index resolution moved outside per-block `par_iter` loop into `IndexedBatches` struct. Eliminates per-block `Vec<String>` allocation, `HashSet` construction, and `schema().index_of()` lookups.
