I need to build a new super-performant query engine allowed to query arbitrary parquet files and do joins.

We have current implementation but schema for dataset is hardcoded and

- we cant use any code from that repo directly
- we need more flexible solution to add new datasets types faster - each new schema requires manual code

You can use this old only for understanding how it works and writing tests
https://github.com/subsquid/data/blob/5b2c0bdbab86945934b88604f5728ec5179ecc4d/crates/query/benches/query/

First of all, analyze this parquet files to understand their structure.
Prepare a metadata files for both cases, which include description of the parquet structure, ordering keys, etc
We have a references queries in a legacy format to test:

For solana we have a benchmark located
https://github.com/subsquid/data/blob/5b2c0bdbab86945934b88604f5728ec5179ecc4d/crates/query/benches/query/mod.rs#L11
The new query engine must be faster.

Prepare a step-by-step plan and think what we can speed it up.

