# GridDB CE 5.7

## Changes in V5.7

The main changes in GridDB CE version 5.7 are as follows:

- SQL memory usage monitoring function

    When executing SQL, GridDB uses working memory for various calculations and processing. The amount of this working memory usage is monitored, and if excessive memory is required to the extent that it hinders the stable operation of GridDB, the SQL is automatically stopped and the SQL is output to a log.

- Reduced database recovery time

    Even if a failure occurs, GridDB executes a recovery process to restore the data that was registered up to that point. The function has been enhanced so that this process can be executed in parallel. This has shortened the recovery time compared to before, and made it possible to start a node quickly for large amounts of data.

- WebAPI Enhancements

    Previously, WebAPI only supported some data operations such as data registration, search, etc. Now, DDL and DML functions have been added, allowing a full range of data operations to be performed using only WebAPI.
    
- Python SQL interface

    Added sample codes for the Python standard DBAPI2, a widely recognized API for database access in the Python programming language. 

---

## SQL memory usage monitoring function

### Settings for GridDB Server

Set the following parameters in the node definition file (gs_node.json).

  * /sql/failOnTotalMemoryLimit：If it is set to true, the SQL requiring memory beyond the limit is automatically stopped.
  * /sql/totalMemoryLimit：Upper working memory limit for processing a SQL
  
### Added meta-table

We added meta-tables about the following statistics.
- Resource statistics for the running statement (SQL): #statement_resources
- Resource statistics for the running distributed task: #task_resources

## Reduced database recovery time

### Settings for GridDB Server

Set the following parameters in the node definition file (gs_node.json).

  * /dataStore/recoveryConcurrency：Specify the concurrency of recovery processing.

## WebAPI Enhancements

Please refert to the following URL.  
https://github.com/griddb/webapi

## Python SQL interface

Please refert to the following URL.  
https://github.com/griddb/jdbc/tree/master/sample