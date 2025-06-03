
# Introduction

## Purpose of this Document

This document explains how to set up GridDB Monitoring Template for Zabbix and how to monitor applying the Template

## Note

GridDB Monitoring Template for Zabbix is a template that helps you monitor GridDB.
Customize the Template to use it for your system to operate.

# Overview

## What is GridDB Monitoring Template for Zabbix?

GridDB Monitoring Template for Zabbix is a template that helps you monitor GridDB in Zabbix in a number of ways including alive monitoring, resource monitoring, and performance monitoring.

## File configuration

This Template consists of the following file:

-   `griddb_templates.xml`
    -   monitoring template for Zabbix version 4.0/5.0
-   `griddb_templates_v6.xml`
    -   monitoring template for Zabbix version 6.0
-   `griddb_templates_v7.xml`
    -   monitoring template for Zabbix version 7.0

## Working environment

We have confirmed GridDB Monitoring Template with the following software:

-   Zabbix version 6.0/7.0
-   GridDB V5.8CE
    -   Server
    -   Operating commands (gs_stat)
    -   WebAPI

Also, install Zabbix Agent on each server running the monitored GridDB node.

The following instructions assume the above software is installed and servers to be monitored are registered as hosts in Zabbix.

[note]
-   A GridDB version other than the one shown in the bullet list above may not be able to retrieve some of the items.

# How to install

## Setting the Zabbix agent

This Monitoring Template uses ActiveCheck to retrieve GridDB event logs. To activate ActiveCheck, enter the following in the settings file. ( The default file is `/etc/zabbix/zabbix_agentd.conf` for the Zabbix agent. )

|setup value  | description|
|-|-|
|Server | address of the Zabbix server|
|Hostname     | host name set in Zabbix|
|ServerActive | address of the Zabbix server|

After changing the settings, restart the Zabbix agent.

## Importing a template

Log in to the Zabbix frontend and perform the following steps to import the Monitoring Template:

1. Select Configuration →Templates →Import.
2. Specify `griddb_templates.xml` as the import file.
3. Click on Import with the given default rules.

If the Template is successfully imported, the template [Template GridDB] will be added to the list.

## Setting template macros

Select [Template GridDB] →Macros tab and change the default for each macro to match GridDB settings.

|macro | default | description|
|-|-|-|
|{$GSHOME}         | `/var/lib/gridstore`                                 | GridDB home directory|
|{$GSLOG}          | `/var/lib/gridstore/log`                             | GridDB event log storage directory|
|{$GSHOSTGROUP}    | GridDB nodes                                         | Zabbix host group name|
|{$GSHOSTPORT}     | 10040                                                | port number for the operational management of GridDB nodes|
|{$GSUSER}         | admin                                                | administrative user of GridDB clusters|
|{$GSPASS}         | admin                                                | password for an administrative user of GridDB clusters|
|{$GSPARTITIONNUM} | 128                                                  | number of partitions|
|{$GSWEBAPIURL}    | http://localhost:8081/griddb/v2/myCluster/dbs/public | URI for the GridDB WebAPI|

[note]
-   To set different values to each host, set macros in each host; which will then be used instead of template macros.


## Setting a template to a host

Monitoring starts once the Monitoring Template is set to a host where the Zabbix agent and the GridDB server are installed.

Follow the steps below to set the Template:

1. Go to Configuration → Hosts and select the target host and open the configuration screen for the host.
2. Switch to the Templates tab and click on Link new templates.
3. Select Template GridDB and Add → Save.

Saving template settings will automatically start monitoring.
To view the results of monitoring, go to Monitoring → Latest data section and select the target host in the list displayed.

# What the Monitoring Template monitors

## Applications

The following applications are currently available:

|name | Overview|
|-|-|
|gs_stat | set of items for performance information that can be obtained using the gs_stat command|
|gs_logs | set of items concerning the GridDB server log|
|gs_aggregation | set of items that aggregates data for host groupset of items that aggregates data for host group|

## Items

This section describes items in each application.

### gs_stat

|name | type | monitoring interval | Overview|
|-|-|-|-|
|[GridDB] gs_stat master | HTTP agent | 30 sec. | retrieves JSON-format performance information from nodes; used as a master file of miscellaneous performance information items|
|[GridDB] (JSON Path) | dependent item | - | miscellaneous performance information items|
|(JSON Path).diff | dependent item | - | items that calculate the differences between the previous and current cumulative values, from among miscellaneous performance information.|

For details about gs_stat items, see the GridDB Features Reference.

To enable SSL connection, enter the following settings:
* Set the macro `{$GSHOSTPORT}` to`/system/serviceSslPort` (default: 10045) of the node.
  
* Change the URL for the item `[GridDB] gs_stat master` from `http://` to `https://`.

### gs_logs

|name | type | monitoring interval | Overview|
|-|-|-|-|
|[GridDB] Event logs | Zabbix agent (active) | 1 sec.              | collects event log files|
|[GridDB] Event logs INFO | Zabbix agent (active) | 1 sec.              | collects INFO logs|
|[GridDB] Event logs WARNING | Zabbix agent (active) | 1 sec.              | collects WARNING logs|
|[GridDB] Event logs ERROR | Zabbix agent (active) | 1 sec.              | collects ERROR logs|
|[GridDB] Periodic checkpoint elapsed time | Zabbix agent (active) | 10 sec.             | retrieves from logs the elapsed time for periodic checkpoint execution|
|[GridDB] Slow query logs | Zabbix agent (active) | 1 sec.              | collects slow query logs|

### gs_aggregation

gs\_aggregation items perform aggregation operations for the host group specified in {$GSHOSTSGROUP} to collect cluster-level information.


|name | type | monitoring interval | Overview|
|-|-|-|-|
|[GridDB] Owner partition count  | Zabbix aggregate | 30 sec.             | number of owners among cluster replicas|
|[GridDB] Backup partition count | Zabbix aggregate | 30 sec.             | number of backups among cluster replicas|
|[GridDB] Store total use        | Zabbix aggregate | 30 sec.             | capacity (in bytes) of all the data owned by a cluster|

## Triggers

Set a trigger for a monitoring item to detect and report incidents and events related to GridDB.

|name | severity | requirements | Overview|
|-|-|-|-|
|[GridDB] OWNER_LOSS partition has been detected.            | High        | patitionStatus has transitioned to OWNER LOSS.               | reports problems with partitions.|
|[GridDB] ABNORMAL node has been detected.                    | High        | nodeStatus has transitioned to ABNORMAL.                     | reports problems with nodes.|
|[GridDB] Log duplication has stopped due to some error.      | Average     | duplicateLog has changed to -1.                              | reports automatic backups have stopped due to some error.|
|[GridDB] Some error has been detected on {HOST.NAME}.        | Average     | Logs containing the string ERROR have been detected.         | reports an error output in an event log.|
|[GridDB] REPLICA\_LOSS partition has been detected.          | Warning     | partitionStatus has transitioned to REPLICA\_LOSS.           | reports changes in partition status.|
|[GridDB] Node has left the cluster.                          | Warning     | clusterStatus has transitioned to SUB_CLUSTER.              | reports a node has left a cluster.|
|[GridDB] Cluster status has become stable.                   | Information | activeCount has changed to be equivalent to designatedCount. | reports cluster status has become stable.|
|[GridDB] Total number of nodes in the cluster has decreased. | Information | designatedCount has decreased.                               | reports reduction in cluster size.|
|[GridDB] Total number of nodes in the cluster has increased. | Information | designatedCount has increased.                               | reports expansion in cluster size.|

[note]
-   For those triggers whose severity is other than Information, PROBLEM event generation mode is set to Multiple.

## Graphs

The Monitoring Template provides custom graphs that summarize multiple items to position them on a screen and a dashboard.

|name | type | use|
|-|-|-|
|[GridDB] Cluster health                 | Exploded | for checking whether a cluster is stable; only master nodes are displayed.|
|[GridDB] Store memory usage             | Exploded | for grasping the amount of memory used divided by the memory limit for data management, as a percentage.|
|[GridDB] storeDetail.***             | Exploded | for grasping detailed store information.|
|[GridDB] Network status                 | Normal   | for displaying the current network conditions.|
|[GridDB] Total read and write operation | Normal   | for displaying the (total) number of data Reads/Writes|
|[GridDB] Total checkpoint and backup    | Normal   | for displaying the (total) number of data Reads/Writes|
|[GridDB] memoryDetail total             | Stacked  | for grasping the breakdown of the total amount of memory attached.|
|[GridDB] memoryDetail cached            | Stacked  | for grasping the breakdown of the amount of cached memory.|

## Screens

The Monitoring Template also provides screens that summarize items and graphs on node information. These screens can be viewed from a host screen for each host.

|name | Overview|
|-|-|
|[GridDB] Node status   | displays items and custom graphs concerning node status.|
|[GridDB] Store details | displays custom graphs for detailed store information on one screen.|

# Applications of the Monitoring Template

As applications of the Monitoring Template, this chapter describes how to add a monitoring item and how to create a dashboard.

## Adding a monitoring item using GridDB WebAPI

GridDB WebAPI enables you to execute any SELECT statement to obtain the result in JSON format.
You can also monitor the status of GridDB clusters by aggregating data in miscellaneous meta tables using SQL.

In Zabbix, create monitoring items as indicated in the table below to monitor the status of GridDB cluster, using GridDB WebAPI and meta tables.

|item type | item to create|
|--|--|
| HTTP agent     | executes an SQL statement through the API.|
| dependent item | treats the above HTTP agent item as a master item and extracts a parameter using a JSON Path in preprocessing.|

These two items allow you to perform more flexible monitoring in Zabbix;a separate JDBC application specifically for status collection is not needed.

The Monitoring Template comes with the following application as a reference.

|name | Overview|
|-|-|
|gs_webapi | set of items that retrieve information through the GridDBWebAPI.|

gs_webapi includes the following items. These items store cluster-level information; activate these items for one host only.

|name | item type | description|
|--|--|--|
|[GridDB] Query count master | HTTP agent     | aggregates the total number of meta table #sqls queries|
|[GridDB] Query count        | dependent item | total number of running queries|

<figure>
<img src="img/zbx_qcount.png" alt="total number of running queries"/>
<figcaption>total number of running queries</figcaption>
</figure>

## Creating a dashboard

Dashboards cannot be included in a template. To fully utilize the Monitoring Template in the actual monitoring system, you need to create a widget or position template graphs on a dashboard, among others.

Moreover, many of the items included in the Monitoring Template display node-level information.
To see cluster-level information, utilize a dashboard widget.

Below are the configuration examples of a widget and a dashboard which includes this widget.

### Example of widget configuration

#### process memory history

|||
|-|-|
|widget type | Graphs|
|item name   | [GridDB] processMemory|
|use         | for grasping a summary of memory usage|

<figure>
<img src="img/zbx_pmemory.png" alt="process memory"/>
<figcaption>process memory</figcaption>
</figure>

#### overview of node status

|||
|-|-|
|widget type | plain text|
|item name   | (any item)|
|use         | for grasping node status|

<figure>
<img src="img/zbx_nodestatus.png" alt="node status"/>
<figcaption>node status</figcaption>
</figure>

#### number of reference rows

|||
|-|-|
|widget type | Graphs|
|item name   | [GridDB] totalRowRead.diff|
|use         | for grasping changes in load due to disk reads|

<figure>
<img src="img/zbx_rowread.png" alt="number of reference rows"/>
<figcaption>number of reference rows</figcaption>
</figure>

#### number of registration rows

|||
|-|-|
|widget type | Graphs|
|item name   | [GridDB] totalRowWrite.diff|
|use         | for grasping changes in load due to disk writes|

<figure>
<img src="img/zbx_rowwrite.png" alt="number of registration rows"/>
<figcaption>number of registration rows</figcaption>
</figure>

#### elapsed time for periodic checkpoint execution

|||
|-|-|
|widget type | Graphs|
|item name   | [GridDB] Periodic checkpoint elapsed time|
|use         | for grasping changes in load due to disk writes|

<figure>
<img src="img/zbx_cptime.png" alt="elapsed time for periodic checkpoint execution"/>
<figcaption>elapsed time for periodic checkpoint execution</figcaption>
</figure>

#### slow query logs

|||
|-|-|
|widget type | plain text|
|item name   | [GridDB] Slow query logs|
|use         | for analyzing causes of a slowdown if there is one.|

<figure>
<img src="img/zbx_slowquery.png" alt="slow query logs"/>
<figcaption>slow query logs</figcaption>
</figure>

### Example of dashboard configuration

#### for cluster monitoring

<figure>
<img src="img/zbx_db.png" alt="Example of dashboard configuration (cluster monitoring)" width="100%"/>
<figcaption>Example of dashboard configuration (cluster monitoring)</figcaption>
</figure>

To monitor an entire cluster, display graphs that aggregate information on each node, information on event logs, load on each node, and resource usage.

Moreover, you could also use items included in the Template OS Linux and item keys for the Zabbix agent to display information on OS resources together in addition to the information above, which will be useful for identifying bottlenecks.

Additionally, it is recommended to configure a cluster in such a way that gives you a visual representation of incident status at a glance by fully utilizing various Zabbix features including action logs, incident information, and maps.

#### for node monitoring

<figure>
<img src="img/zbx_db_node.png" alt="Example of dashboard configuration (node monitoring)" width="100%"/>
<figcaption>Example of dashboard configuration (node monitoring)</figcaption>
</figure>

It is also recommended to create a dash board for node monitoring in addition to cluster monitoring.

A dashboard for node monitoring aggregates and displays more detailed information about nodes, including node event logs, breakdowns of memory usage, and disk space. Such information will be useful for cause analysis when specific nodes are highly loaded or a node failure occurs.

Set each widget as a Dynamic item; this will allow you to switch nodes to display by selecting Host on the upper right-hand side of the window.


# Trademarks

-   GridDB is a registered trademark for Toshiba Digital Solutions Corporation in Japan.
-   Zabbix is a registered trademark for Zabbix SIA.
