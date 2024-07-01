# References Tests

This is a set of tests that use the reference files from https://github.com/apache/parquet-testing/.

## Updating the Reference Files

This assumes that parquetjs is in the same folder as the clone of parquet-testing.

1. `git clone git@github.com:apache/parquet-testing.git`
1. `cd ../parquetjs`
1. `cp ../parquet-testing/data/*.parquet ./test/reference-test/files/`
