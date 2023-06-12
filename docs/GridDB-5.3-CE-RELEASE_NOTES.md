# GridDB CE 5.3

## Changes in V5.3

Main changes in GridDB CE V5.3 are as follows:

- Functionality Enhancement for Timeseries data management and analysis

    - Nanosecond (billionth of a second) Data Processing

    In the previous version of GridDB, data can be stored up to millisecond (1/1000th of a second) accuracy. From GridDB 5.3 onwards, data can now be stored at higher granularity at nanosecond accuracy. This allows data to be collected and analyzed at one million times higher resolution than that of the conventional system, enabling more accurate analysis and forecasting. (This feature is currently supported by the Java client, C client and JDBC driver.)

    - SQL Enhancement (Aggregation/Interporation operator for timeseries data)

    Time series data can now be grouped into time intervals and combined with aggregation functions to extract the maximum, minimum, average, and total values of these intervals. This function has been incorporated directly into GridDB to facilitate and speed up time series data analysis as it eliminates the need to extract and aggregate data on the application side. And a function that automatically interpolates and fills in missing values using linear interpolation has been developed. This allows for data continuity and enables highly accurate analysis and forecasting. (This feature is currently supported by the JDBC driver.)
 
---

## Nanosecond Data Processing

### Usage for High precision TIMESTAMP

Please refer to the following sample codes creating container, putting row data and getting row data with nanosecond.

- Java Client: NanoTimestamp.java
- C Client: NanoTimestamp.c
- JDBC Driver: NanoTimestamp-SQL.md

Note: The row-key for Collection container supports 3 type (MILLI/MICRO/NANO-SECOND) TIMESTAMPs.
 But The row-key for Timeseries container supports only MILLISECOND TIMESTAMP.

## SQL enhancement (Aggregation/Interporation operator for timeseries data)

### Syntax

The syntax "GROUP BY RANGE" is added in GridDB SQL.

``` example
SELECT <expr_list> FROM <table_list> WHERE <range_cond>
GROUP BY RANGE(<key>) EVERY(<interval>, <unit>[, <offset>]) [FILL(<fill_opt>)]
```

- key: Column name (TIMESTAMP type only) for aggregation/interporation
- interval: Interval value for aggregation/interporation
- unit: Unit of interval value for aggregation/interporation (DAY, HOUR, MINUTE, SECOND, MILLISECOND)
- offset: Starting offset
- fill_opt: Interporation method (LINEAR, NONE, NULL, PREVIOUS)

Note: Available functions in expr_list: AVG, COUNT, MAX, MIN, SUM, TOTAL, GROUP_CONCAT, STDDEV, MEDIAN, PERCENTILE_CONT and so on.

### Example for Aggregation operator (SQL)

``` example
SELECT key,avg(value) FROM table1 
WHERE key BETWEEN TIMESTAMP('2023-01-01T00:00:00Z') AND TIMESTAMP('2023-01-01T00:01:00Z')
GROUP BY RANGE (key) EVERY (20, SECOND)
```

### Example for Interporation operator (SQL)

``` example
SELECT * FROM table1 
WHERE key BETWEEN TIMESTAMP('2023-01-01T00:00:00Z') AND TIMESTAMP('2023-01-01T00:01:00Z')
GROUP BY RANGE (key) EVERY (10, SECOND) FILL (LINEAR)
```
