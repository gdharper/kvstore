# kvstore
Persistent, thread-safe key value store, implemented as a header-only c++ library.

Design draws inspiration from Facebook (now Meta)'s [RocksDB](https://github.com/facebook/rocksdb),
as well as other sources from the web.
Uses a mix of in-memory and file-backed storage to ensure data can grow to large sizes while continuing to serve requests performantly. Data is first written/read from an in-memory memtable. once this table fills up, it is saved (still in memory) to a read only buffer. This buffer is periodically flushed to files on disk by a background thread.

## features
 - Implements 2 APIs:
    - **put**: takes a string key and an  value and stores the value under the key
    - **get**: takes a string key and returns the corresponding value if previosuly stored via "put"
 - Fully thread-safe and consistent - utilizes a lock-free, skiptable-based memtable implementation and fully-thread-safe SST files to serve requests.
 - Fully persistent- uses a thread-safe write-ahead-log to persist in-memory data across process crashes.

## usage
See "tool.cpp" for a simple usage example

## todo
- implement "delete" to remove keys
- integrate bloom filter (implementation complete) into the SST file for fast rejection of absent "get" operations
- add a caching layer for frequently accessed keys
- implement compaction of SST files to combine duplicate entries, reduce file scan time, and reduce disk utilization
- implement compression for stored keys/values
- enable encryption of stored data
- enable batch transations
