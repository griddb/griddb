# GridDB CE 5.6

## Changes in V5.6

The main changes in GridDB CE version 5.6 are as follows:

- Compression algorithm enhancement

    Strengthening of function to increase data compression rates, reduce storage costs and enable high-speed access.

- Interval partitioning enhancement

    Enhancement of function to distribute data into smaller units and increase processing parallelism.

- Automatic Time-Series Data Aggregation

    The aggregation and calculation of metrics such as average, maximum, and deviation at regular intervals, which were previously performed on-demand during data analysis, can now be automated by registering them using GridDB CLI(gs_sh). This significantly reduces the effort and time required for data analysis.
    
---

## Compression algorithm enhancement

It is now possible to select compression algorithm from the following 2 types.

- ZLIB compression: compression with ZLIB library
- ZSTD compression: compression with ZSTD(Zstandard) library (since V5.6)

### Settings for GridDB Server

Set the following parameters in the node definition file (gs_node.json).

  * dataStore/storeCompressionMode：Set the following string.
    - "NO_COMPRESSION": compression disabled.
    - "COMPRESSION_ZLIB" or "COMPRESSION": ZLIB compression enabled.
    - "COMPRESSION_ZSTD": ZSTD compression enabled. It is usually faster and has a higher compression ratio than ZLIB compression.

### Notation

GridDB V5.6 can load DB file created with GridDB CE V5.X before V5.5.
But after GridDB start with COMPRESSION_ZSTD mode, GridDB CE of the version before V5.5 can't load the DB file.

## Interval partitioning enhancement

When we create an interval (hash) partitioning table, we can specify hour as the interval unit.

### Syntax

Example for creating an interval partitioned table:

- Table (collection)

  |                                   |
  |-----------------------------------|
  | CREATE TABLE \[IF NOT EXISTS\] table_name ( column definition \[, column definition ...\] \[, PRIMARY KEY(column name \[, ...\])\])<br>\[WITH (property_key=property_value, ...)\]<br>PARTITION BY RANGE(column_name_of_partitioning_key) EVERY(interval_value \[, interval_unit \]); |

- Timeseries table (timeseries container)

  |                                   |
  |-----------------------------------|
  | CREATE TABLE \[IF NOT EXISTS\] table_name ( column definition \[, column definition ...\] )<br>USING TIMESERIES \[WITH (property_key=property_value, ...)\]<br>PARTITION BY RANGE(column_name_of_partitioning_key) EVERY(interval_value \[, interval_unit \]) ; |

If the column of TIMESTAMP (including TIMESTAMP with specified precision) is specified, the interval unit should also be specified. DAY or HOUR is the value that can be specified as the interval unit.

## Automatic Time-Series Data Aggregation

Time-Series Data Aggregation is realized with script files for GridDB CLI.
Automatic Time-Series Data Aggregation is realized by executing the script files regularly.
