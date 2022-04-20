# GridDB CE 5.0

## Changes in V5.0

Main changes in GridDB CE V5.0 are as follows

### Performance improvement for large-scale data

1. Core scaling improvement

    - By restarting a node, concurrency (/dataStore/concurrency in the node definition file) can now be changed.

2. Faster scan and data deletion

    - By setting the string **#unique** as hint information for data affinity, it is now possible to occupy blocks on a per container (table) basis to place data.

3. Reduction of the disk I/O load when performing a checkpoint

    - It is now possible to write logs in several smaller batches during a checkpoint. The number of batches (splits) can be modified by /checkpoint/partialCheckpointInterval in the node definition file. It is 5 by default. While increasing the settings value can decrease the amount of writing to a checkpoint log file at a time, this may increase the recovery time during a startup.

### Functionality enhancement

4. Renaming a column using SQL

    - The following SQL is supported:

    ALTER TABLE *table name* RENAME COLUMN *column name before renaming* TO *column name after renaming*;

---

## Specification Changes

### Database files placement

Prior to V5, checkpoint file and transaction log file are placed on the "data" directory.

In V5, the arrangement is as follows: 

  * Conventional checkpoint file is divided into data file (extension: "dat") for writing data and checkpoint log file (extension: "cplog") for writing block management information.
  * Data files and checkpoint log files are written to the "data" directory, and the transaction log files are written to  the "txnlog" directory.
  * Within the "data" ("txnlog") directory, a directory is created for each partition and files are placed in the partition directory.

### Added Parameters

The following parameters are added to the node definition file (gs_node.json).

  * /dataStore/transactionLogPath : Specify the full or relative path to the location directory for a transaction log file. The default value is txnlog.

  * /checkpoint/partialCheckpointInterval : Specify the number of splits in write processing of block management information to checkpoint log files during a checkpoint.

### Notations

- Current version is not compatible with the previous GridDB versions.
- Hash index, Row expiry release, Timeseries compression and Trigger function have been discontinued in V5.
Please use Tree index, Partition expiry release, and Block data compression instead of Hash index, Row expiry release, and Timeseries compression.

