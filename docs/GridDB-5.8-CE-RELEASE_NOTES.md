# GridDB CE 5.8

## Changes in V5.8

The main changes in GridDB CE version 5.8 are as follows:

- SQL Optimization Enhancements

    Supported Join Optimization for the driving table, the inner table of SQL execution. The join plan can be generated with the cost-based method.

- Timeseries-data Functionality Enhancements

    Supported moving average calculation for SQL analytic functions. And improved the accuracy of date arithmetic functions.

---

## SQL Optimization Enhancements

It is now possible to switch between cost-based and rule-based methods to select the driving table and the inner table for join operation, and this can be configured using the following:
- Cluster definition file (gs_cluster.json)
- Hint phase

Using the cluster definition file (gs_cluster.json):

  * /sql/costBasedJoinDriving：Specifies whether to use the cost-based method for determining the driving table for join during SQL plan generation. If set to false, it uses the rule-based method. The default value is true, which indicates the cost-based method.


Using the hint phase：

  * CostBasedJoinDriving()：Use the cost-based method for determining the driving table for join.
  * NoCostBasedJoinDriving()：Use the rule-based method for determining the driving table for join.

## Timeseries-data Functionality Enhancements

### Supported moving average calculation for SQL analytic functions


Added FRAME clause for SQL analytic functions

``` example
    function OVER ( [PARTITION BY expression1 ] [ ORDER BY expression2 ] [ FRAME-clause ] )
```

The syntax of FRAME-clause is as bellow:

``` example
    ROWS | RANGE <FRAME-start-value> | BETWEEN <FRAME-start-value> AND <FRAME-end-value>
```

The syntax of the start value and the end value of Frame is as bellow:

``` example
    UNBOUNDED PRECEDING | UNBOUNDED FOLLOWING | CURRENT ROW | <Frame-boundary1> PRECEDING | <Frame-boundary2> FOLLOWING 
```

- CURRENT ROW: Specify the current row to be analyzed
- UNBOUNDED: Specify the head or the tail of the partition
- PRECEDING/FOLLOWING: Specify preceding or following

The syntax of the boundary of Frame is as bellow:

``` example
    value1 | ( value2, unit )
```

- The following functions can be specified the FRAME-clause.

    AVG、COUNT、MAX、MIN、SUM、TOTAL、STDDEV、VAR

- The following functions can not be specified the FRAME-clause.

    ROW_NUMBER、LAG、LEAD

例)

Calculate the moving average for the previous 10 rows.

``` example
SELECT
    AVG(value1)
        OVER(ORDER BY time) 
        ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
FROM tbl1;
```

Calculate the moving average for the period up to 10 minutes prior to each row.

``` example
SELECT
    AVG(value1)
        OVER(ORDER BY time)
        RANGE BETWEEN (10, MINUTE) PRECEDING AND CURRENT ROW
FROM tbl1;
```

### Improved the accuracy of date arithmetic functions

Added the following functions for SQL and TQL(query language for NoSQL Interface).

- SQL
``` example
  * TIMESTAMP_MS(timestamp_string [, timezone])：Converts a string representation of the time to a TIMESTAMP(3) type with millisecond precision.
  * TIMESTAMP_US(timestamp_string [, timezone])：Converts a string representation of the time to a TIMESTAMP(6) type with microsecond precision.
  * TIMESTAMP_NS(timestamp_string [, timezone])：Converts a string representation of the time to a TIMESTAMP(9) type with nanosecond precision.
```

- TQL
``` example
  * TIMESTAMP_MS(str)：Converts a string representation of the time to a TIMESTAMP(3) type with millisecond precision.
  * TIMESTAMP_US(str)：Converts a string representation of the time to a TIMESTAMP(6) type with microsecond precision.
  * TIMESTAMP_NS(str)：Converts a string representation of the time to a TIMESTAMP(9) type with nanosecond precision.
```

And The following functions now support calculations with microsecond and nanosecond precision:

- SQL
``` example
  * TIMESTAMP_ADD(time_unit, timestamp, duration [, timezone])/TIMESTAMP_DIFF(time_unit, timestamp1, timestamp2 [, timezone])
```

- TQL
``` example
  * TIMESTAMPADD(time_unit, timestamp, duration)/TIMESTAMPDIFF(time_unit, timestamp1, timestamp2)
  Note: We have also added TIMESTAMP_ADD()/TIMESTAMP_DIFF(), which have the same names as the SQL functions.
```

