# GridDB CE 4.3

## Changelog

Main changes in GridDB CE v4.3 are as follows:

#### Scale Up Enhancement
1. Larger Block Size
    - Selectable block size increased to 32MB. A larger block size means more data can be managed per node, and a better performance in scanning large amount of data can be achieved.
2. Split Checkpoint File Placement
    - Checkpoint files can be split and distributed on multiple directories, thus increasing the amount of data that can be managed per node. Disk access performance also improves.
3. Network Interface Isolation
    - Different network interface can be set for external communication between client and nodes, and internal communication between nodes. As a result, network load can be distributed.

#### Performance Improvement
4. Composite Index
    - Index can be set across multiple columns. Using composite index can improve performance.

#### Enhanced Functionality
5. Composite RowKey
    - Rowkey can be set across multiple columns. Values across the columns must be unique. Database design is easier as composite rowkey can be used in place of surrogate key.
6. TimeZone
    - TimeZone's timestamp expression can now be specified with "+hh:mm" or "-hh:mm" format.
