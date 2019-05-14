# GridDB CE 4.0

## Enhanced flexibility and usability

Major updates in GridDB CE V4.0 are as follows:  

1. Improvement in large-scale data management
    - GridDB can now accomodate a bigger database size, up to 50TB per node.
2. Partial execution mode for Query Result
    - It is now possible to get a large query results by dividing the data range.
3. Expansion of characters used for naming objects
    - It is now easier to integrate GridDB with other NoSQL database systems.
4. Database file size reduction functionality
    - Added Linux file block deallocation function to reduce database file size.
5. Change of license type for C Client
    - Changed from AGPL V3 to Apache V2.

---

### 1. Improvement in large-scale data management

The maximum database size per node in conventional scale-out database is typically around several TB per node. 
With this update, GridDB can now accomodate a bigger database size, up to 50TB per node. This corresponds to a fewer number of nodes. 

### 2. Partial execution mode for Query Result

For a large search result, by using the "Partial Execution Mode" function,  it is now possible to divide the data range to ensure that the buffer size used for sending and receiving the query results stay within a certain range.

When you use Java client, the partial execution mode(PARTIAL_EXECUTION) can be set with setFetchOption() in Query class.
Example. query.setFetchOption(FetchOption.PARTIAL_EXECUTION, true);

In this version, the partial execution mode can be used for queries satisfying all the following conditions. And it can be used in combination with LIMIT option. Even if the conditions are not satisfied, errors may not be detected when setting the fetch option.
- The query must be specified by TQL.
- The SELECT clause must be consisted of only '*' and an ORDER BY clause must not be specified.
- The target Container must have been set to the auto commit mode at each partial execution of the query.


### 3. Expansion of characters used for naming objects (e.g. container names).

Permitted characters to be used in cluster names, container names, etc. have been expanded.   
Special characters (hyphen '-', dot '.', slash '/', equal '=') are now supported.  

It is now easier to integrate GridDB with other NOSQL database systems, because object names of other systems can now be used in without rewriting the names.


### 4. Database file size reduction functionality 

This function allows reduction of database file size (disk space) by using Linux file block deallocation processing on unused block areas of database files (checkpoint files). 

Use this function in the following cases. 
- A large amount of data has been deleted.
- There is no plan to update data but there is a necessity to keep the DB for a long term. 
- The disk becomes full when updating data and reducing the DB size temporarily is needed. 

Specify the deallocation option, --releaseUnusedFileBlocks, of the gs_startnode command, when starting GridDB nodes. 

The unused blocks of database files (checkpoint files) are deallocated when starting the node. This will remain deallocated until there is a data update. 

The support environment is the same as block data compression. 

### 5. Change of license type for C Client

GridDB's client have been published under AGPL V3 or Apache V2 licenses. 

With this change, all GridDB's client will be licensed under the Apache License, Version 2.0. 

---

Copyright (C) 2018 TOSHIBA Digital Solutions Corporation
