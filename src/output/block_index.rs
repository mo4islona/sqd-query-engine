use arrow::array::*;
use arrow::record_batch::RecordBatch;
use rustc_hash::FxHashMap;
use std::collections::HashSet;

/// Read a block number column value as u64, handling signed parquet physical types.
/// Parquet stores UInt32/UInt64 as Int32/Int64 physical types, so we reinterpret
/// the bit pattern rather than sign-extending.
fn read_block_number(col: &dyn Array, row: usize) -> Option<u64> {
    if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
        Some(a.value(row))
    } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
        Some(a.value(row) as u64)
    } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        Some(a.value(row) as u64)
    } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        Some((a.value(row) as u32) as u64)
    } else {
        None
    }
}

/// Compute the actual min/max block numbers from scan results (for cross-table pruning).
pub(crate) fn compute_block_range(
    batches: &[RecordBatch],
    bn_column: &str,
) -> (Option<u64>, Option<u64>) {
    let mut min_block: Option<u64> = None;
    let mut max_block: Option<u64> = None;

    for batch in batches {
        if let Some(col) = batch.column_by_name(bn_column) {
            for i in 0..col.len() {
                if let Some(bn) = read_block_number(col.as_ref(), i) {
                    min_block = Some(min_block.map_or(bn, |m: u64| m.min(bn)));
                    max_block = Some(max_block.map_or(bn, |m: u64| m.max(bn)));
                }
            }
        }
    }
    (min_block, max_block)
}

/// Build an index mapping block_number -> list of (batch_index, row_index).
pub(crate) fn build_block_index(
    batches: &[RecordBatch],
    bn_column: &str,
) -> FxHashMap<u64, Vec<(usize, usize)>> {
    let mut index: FxHashMap<u64, Vec<(usize, usize)>> = FxHashMap::default();
    for (batch_idx, batch) in batches.iter().enumerate() {
        if let Some(col) = batch.column_by_name(bn_column) {
            for row in 0..col.len() {
                if let Some(bn) = read_block_number(col.as_ref(), row) {
                    index.entry(bn).or_default().push((batch_idx, row));
                }
            }
        }
    }
    index
}

/// Collect block numbers from batches into a set.
pub(crate) fn collect_block_numbers(
    batches: &[RecordBatch],
    bn_column: &str,
    block_numbers: &mut HashSet<u64>,
) {
    for batch in batches {
        if let Some(col) = batch.column_by_name(bn_column) {
            for i in 0..col.len() {
                if let Some(bn) = read_block_number(col.as_ref(), i) {
                    block_numbers.insert(bn);
                }
            }
        }
    }
}

/// Collect only the first and last block numbers from the blocks table (boundary blocks).
pub(crate) fn collect_boundary_blocks(
    batches: &[RecordBatch],
    bn_column: &str,
    block_numbers: &mut HashSet<u64>,
) {
    let mut min_block: Option<u64> = None;
    let mut max_block: Option<u64> = None;

    for batch in batches {
        if let Some(col) = batch.column_by_name(bn_column) {
            for i in 0..col.len() {
                if let Some(v) = read_block_number(col.as_ref(), i) {
                    min_block = Some(min_block.map_or(v, |m: u64| m.min(v)));
                    max_block = Some(max_block.map_or(v, |m: u64| m.max(v)));
                }
            }
        }
    }

    if let Some(v) = min_block {
        block_numbers.insert(v);
    }
    if let Some(v) = max_block {
        block_numbers.insert(v);
    }
}
