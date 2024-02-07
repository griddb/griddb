# GridDB CE 5.5

## Changes in V5.5

The main changes in GridDB CE version 5.5 are as follows:

- SQL Cost-Based Optimization

    Enhanced optimization of join order when executing searches. Previously, SQL plans followed the description and order of SELECT statements provided by the user and were generated using rules. Therefore, users needed to understand the rules and describe SELECT statements accordingly to generate preferable plans based on table size and filtering conditions. With this enhancement, the process of executing searches now calculates the estimated cost based on the required number of rows (considering table size and filtering conditions) and can determine the optimal join order for multiple tables to generate plans.

- Optimization of index joins related to partitioning tables

    Enhanced execution of index joins as much as possible, even when involving partitioning tables. Previously, there were cases where index joins could not be performed due to internal constraints of GridDB, even if the target table had indexes (such as when the number of table partitions was too large), resulting in table scans. With this enhancement, plans that perform index joins are now generated and executed when possible, regardless of internal details (such as the number and type of table partitions and indexes).

- Support for batch updates in SQL

    Newly added support for JDBC methods (such as addBatch) to perform batch updates.
    
- GridDB Monitoring Template for Zabbix

    Added Zabbix monitoring templates as samples for monitoring GridDB Server. It facilitates comprehensive GridDB monitoring within Zabbix, encompassing functionalities such as live system monitoring, resource utilization tracking, and performance assessment.

---

## SQL Cost-Based Optimization

It is now possible to switch between cost-based and rule-based methods, and this can be configured using the following:
- Cluster definition file (gs_cluster.json)
- Hint phase

### Using the cluster definition file (gs_cluster.json)

  * /sql/costBasedJoin：Specifies whether to use the cost-based method for determining join order during SQL plan generation. If set to false, it uses the rule-based method to determine join order. The default value is true, which indicates the cost-based method.

## Support for batch updates in SQL

Supports the following methods for the PreparedStatement interface. Calling query execution APIs like executeQuery with unset parameters will result in an SQLException.
- addBatch()
- clearBatch()
- executeBatch()
    - Batch updates are only possible for SQL statements that do not include ResultSet, and an error will be returned if they do. The timing of returning an error is not during addBatch()/clearBatch() but during executeBatch(). Additionally, cases where the target SQL is a compound statement will also result in an error.


Please refer to the following sample codes for creating container and inserting row data.
- JDBC Driver: [JDBCAddBatch.java](https://github.com/griddb/jdbc/blob/master/sample/ja/jdbc/DBCAddBatch.java)

## GridDB Monitoring Template for Zabbix

Please refer to "[User Guide for GridDB Monitoring Template for Zabbix](https://github.com/griddb/griddb/blob/master/sample/zabbix/GridDB_ZabbixTemplateGuide.md)".