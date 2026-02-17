# GridDB CE 5.9

## Changes in V5.9

The main changes in GridDB CE version 5.9 are as follows:

- SQL Optimization Enhancements

    Cost-Based Index Scan Optimization: Until now, GridDB has supported cost-based optimization for determining join order and join direction (driving table / inner table) in multi-join operations. With this release, the SQL engine can now determine the optimal index scan method based on cost, using statistical information, and generate and execute a plan accordingly.

- Scheduling Functionality Enhancements

    SQL Resource Scheduling Functionality: The SQL resource scheduling feature schedules SQL processing within a predefined resource range to improve response time and resource utilization efficiency. It consists of the following three sub‑functions: "Processing order control", "Execution control based on memory usage" and "Resource monitoring"

- Timeseries-data Functionality Enhancements

    Row Pattern Recognition Functionality: Support has been added for row pattern recognition standardized in SQL:2016. Searches based on value change patterns (e.g., increasing, decreasing, stable) are now possible.

---

## Cost-Based Index Scan Optimization

There are two methods for determining how index scans are performed during join processing: cost-based and rule-based.

You can select which method to use through:
- Cluster definition file (gs_cluster.json)
- Hints

When using the cluster definition file (gs_cluster.json):

  * /sql/costBasedIndexScan: Specifies whether to use the cost-based method to determine index scan strategy during joins. If set to false, the rule-based method is used. The default value is true (cost-based).

When using hints:

  * CostBasedIndexScan(): Determines index scan strategy using the cost-based method.
  * NoCostBasedIndexScan(): Determines index scan strategy using the rule-based method instead of cost-based.

## SQL Resource Scheduling Functionality

This feature consists of the following three components:
- Processing order control
- Execution control based on memory usage
- Resource monitoring

### Processing Order Control

The new scheduler assigns tasks as evenly as possible across SQL statements.
By distributing tasks fairly at the SQL level, it reduces resource contention and queue buildup, stabilizing overall response performance.

Configured using the following parameter in the node definition file (gs_node.json):

  * /sql/resourceControlLevel: Scheduler behavior when failOnTotalMemoryLimit = true
    - 0: Automatic configuration (defaults to level 3 = new scheduler)
    - 1: Legacy scheduler without SQL memory upper limit (equivalent to V5.6)
    - 2: Legacy scheduler with SQL memory upper limit (equivalent to V5.7/V5.8)
    - 3: New scheduler

### Execution Control Based on Memory Usage

By limiting the total memory available for SQL processing on a per‑node basis, the system prevents memory exhaustion and server crashes caused by heavy SQL workloads.

In V5.7, SQL statements determined to require memory beyond the limit began being forcibly terminated.
In V5.9, the new scheduler more accurately controls SQL execution based on the limit and automatically determines which SQL statements to stop, improving stability and control precision.

Configured using the following parameters in the node definition file (gs_node.json):

  * /sql/failOnTotalMemoryLimit: Controls behavior when exceeding the memory limit. When true, SQL statements exceeding the limit are forcibly stopped.
  * /sql/totalMemoryLimit: Total SQL processing memory limit per node. If set to 0, an automatic calculation formula is applied.

### Resource Monitoring

The new scheduler can monitor detailed SQL resource usage (memory, I/O, communication time, etc.) and output it to event logs or meta tables.
This helps identify SQL statements that consume excessive resources or queries that cause system load, aiding both performance tuning and troubleshooting.

Set /trace/resourceMonitor to LEVEL_WARNING in the node definition file (gs_node.json), and configure targets using:

  * /sql/monitoringMemoryRate: Ratio of total memory consumption to monitor (0–1). 0 disables monitoring.
  * /sql/monitoringStoreRate: Ratio of SQL intermediate store usage to monitor (0–1). 0 disables monitoring.
  * /sql/monitoringNetworkRate: Ratio based on total transfer time during monitoring interval (0–1). 0 disables monitoring.
  * /transaction/monitoringStoreRate: Ratio of datastore access volume (estimated) relative to datastore memory limit (0–1). 0 disables monitoring.

Resource usage can be checked through:
- Event logs
- Meta tables
  * #statement_resources
  * #task_resources

### Row Pattern Recognition Functionality

The MATCH_RECOGNIZE clause has been newly added.

Basic syntax:
``` example
MATCH_RECOGNIZE (
  [PARTITION BY expr1 [, expr2 ...]]
  [ORDER BY expr3 [, expr4 ...]]
  [MEASURES expr5 [AS alias1], expr6 [AS alias2], ...]
  [ONE ROW PER MATCH | ALL ROWS PER MATCH]
  [AFTER MATCH SKIP skip-option]
  PATTERN (pattern-specification)
  DEFINE variable1 AS condition1 [, variable2 AS condition2, ...]
)

- PARTITION BY: Specifies grouping units for pattern recognition.
- ORDER BY: Specifies ordering within each group.
- MEASURES: Specifies additional output information for matched patterns.
- PATTERN: Specifies the pattern to detect (regex‑like).
- DEFINE: Specifies evaluation conditions for each pattern variable.
- ONE ROW / ALL ROWS PER MATCH: Controls output granularity (default: ONE ROW).
- AFTER MATCH SKIP: Only PAST LAST ROW is supported.

MEASURES and DEFINE can use MATCH_RECOGNIZE‑specific functions:  
MATCH_NUMBER(), CLASSIFIER(), PREV(), NEXT(), FIRST(), LAST()
```

Example:
``` example
SELECT *
FROM sensor_data
MATCH_RECOGNIZE (
  PARTITION BY device_id
  ORDER BY timestamp
  MEASURES
    FIRST(timestamp) AS start_time,
    LAST(timestamp) AS end_time
  PATTERN (UP+ DOWN+)
  DEFINE
    UP AS value > PREV(value),
    DOWN AS value < PREV(value)
);

device_id   timestamp    value   start_time   end_time     match_no 
-----------+------------+-------+------------+------------+--------- 
devA        2025-09-01   10      2025-09-01   2025-09-05   1 
devA        2025-09-02   12      2025-09-01   2025-09-05   1 
devA        2025-09-03   15      2025-09-01   2025-09-05   1 
devA        2025-09-04   13      2025-09-01   2025-09-05   1 
devA        2025-09-05   11      2025-09-01   2025-09-05   1
```
