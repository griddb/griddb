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

---

### 1. Larger Block Size

Selectable block size are 64KB, 1MB, 4MB, 8MB, 16MB, 32MB. For normal usage, please use the default 64KB block size.

Block size setting can be found in the cluster definition file (gs_cluster.json) under /dataStore/storeBlockSize.

### 2. Split Checkpoint File Placement

Checkpoint files can be split and distributed to multiple directories.

Set the following parameters in the node definition file (gs_node.json).
- /dataStore/dbFileSplitCount: Split number
- /dataStore/dbFilePathList: Location of the checkpoint files.

Ex.)

    ```    
	"dataStore":{
        "dbFileSplitCount": 2,
        "dbFilePathList": ["/stg01", "/stg02"],
    ```    

### 3. Network Interface Isolation

GridDB node supports 2-type of communications for transaction processing:
- external communication between client and nodes
- internal communication between nodes

Prior to this update, both communications used the same network interface. It is now possible to separate the networks.

Set the following parameters in the node definition file (gs_node.json).
- /transaction/serviceAddress: external communication address
- /transaction/localServiceAddress(New): internal communication address

Ex.)

    ```    
    "cluster":{
        "serviceAddress":"192.168.10.11",
        "servicePort":10010
    },
    "sync":{
        "serviceAddress":"192.168.10.11",
        "servicePort":10020
    },
    "system":{
        "serviceAddress":"192.168.10.11",
        "servicePort":10040,
              :
    },
    "transaction":{
        "serviceAddress":"172.17.0.11",
        "localServiceAddress":"192.168.10.11",
        "servicePort":10001,
              :
    },

    ```

### 4. Composite Index

Index can be set across multiple columns for Tree-Index.

When we use Java Client, we can set and get the list of column names (or column identifiers) with the following methods in IndexInfo class.
- void setColumnName(java.lang.String columnName)
- void setColumnList(java.util.List<java.lang.Integer> columns)
- java.util.List<java.lang.String> getColumnNameList()
- java.util.List<java.lang.Integer> getColumnList()

Please refer to (3) in [CreateIndex.java](https://github.com/griddb/griddb_nosql/blob/master/sample/guide/ja/CreateIndex.java).

And when we use C Client, please refer to compositeInfo in [CreateIndex.c](https://github.com/griddb/c_client/blob/master/sample/guide/ja/CreateIndex.c).

### 5. Composite RowKey

Rowkey can be set to multiple consecutive columns from the first column for Collection container.

When we use Java Client, composite rowkey can be set with the following methods in ContainerInfo class.
- void setRowKeyColumnList(java.util.List<java.lang.Integer> rowKeyColumnList)

Ex.)  
    containerInfo.setRowKeyColumnList(Arrays.asList(0, 1));

Please refer to buildContainerInfo() in [CompositeKeyMultiGet.java](https://github.com/griddb/griddb_nosql/blob/master/sample/guide/ja/CompositeKeyMultiGet.java).

And when we use C Client, please refer to rowKeyColumnList in [CompositeKeyMultiGet.c](https://github.com/griddb/c_client/blob/master/sample/guide/ja/CompositeKeyMultiGet.c).

### 6. TimeZone

TimeZone's timestamp expression can now be specified with "+hh:mm" or "-hh:mm" format.

The following TimeZone arguments are added to TimestampUtils class for Java Client.
- add(timestamp, amount, timeUnit, zone)
- format(timestamp, zone)
- getFormat(zone)
