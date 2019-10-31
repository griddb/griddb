<img src="http://griddb.org/Image/GridDB_logo.png" align="center" height="48" alt="GridDB"/>

[![Visit Website](https://img.shields.io/badge/website-visit-orange.svg)](https://griddb.net) 
![GitHub All Releases](https://img.shields.io/github/downloads/griddb/griddb_nosql/total.svg) 
![GitHub release](https://img.shields.io/github/release/griddb/griddb_nosql.svg)
## Overview
  GridDB has a KVS (Key-Value Store)-type data model that is suitable for sensor data stored in a timeseries. It is a database that can be easily scaled-out according to the number of sensors.

  * High Reliability  
    It is equipped with a structure to spread out the replication of key value data among fellow nodes so that in the event of a node failure, automatic failover can be carried out in a matter of seconds by using the replication function of other nodes.

  * High Performance (in-memory)  
   Even if the memory is increased to handle big data in a RDB, it is known that CPU power cannot be fully exploited and only around 10% of CPU resources are allocated to actual data processing due to the large overhead in buffer management, etc. Given the increase in memory size, GridDB minimizes the amount of overheads required previously by lightening the buffer process and recovery process, and by making it lock-free during data processing.

  * Advanced data model and operation model  
    In a traditional distributed KVS, data is handled using operations such as Put/Get/Remove. GridDB expands these functions greatly to support the definition function for organizational data, SQL-like query function, transaction function and Java API (Application Programming Interface) so that RDB users are able to introduce the system smoothly. The key value represents the data in a set of records known as a key container. This is similar to the relationship between a RDB table name and table. It is also equipped with an application function for sensor data management.

  This repository includes server and Java client.

  (Additional infomation)  
  There is [Java client Package (Jar) for v4.0.0 on Maven Central Repository](https://search.maven.org/search?q=g:com.github.griddb) .

## Quick start (Using source code)
  We have confirmed the operation on CentOS 7.6 (gcc 4.8.5) and Ubuntu 18.04 (gcc 4.8.5).

### Build a server and client(Java)
    $ ./bootstrap.sh
    $ ./configure
    $ make 
    
### Start a server
    $ export GS_HOME=$PWD
    $ export GS_LOG=$PWD/log

    $ bin/gs_passwd admin
      #input your_password
    $ vi conf/gs_cluster.json
      #    "clusterName":"your_clustername" #<-- input your_clustername
    $ export no_proxy=127.0.0.1
    $ bin/gs_startnode
    $ bin/gs_joincluster -c your_clustername -u admin/your_password

### Execute a sample program
    $ export CLASSPATH=${CLASSPATH}:$GS_HOME/bin/gridstore.jar
    $ mkdir gsSample
    $ cp $GS_HOME/docs/sample/program/Sample1.java gsSample/.
    $ javac gsSample/Sample1.java
    $ java gsSample/Sample1 239.0.0.1 31999 your_clustername admin your_password
      --> Person:  name=name02 status=false count=2 lob=[65, 66, 67, 68, 69, 70, 71, 72, 73, 74]

### Stop a server
    $ bin/gs_stopcluster -u admin/your_password
    $ bin/gs_stopnode -u admin/your_password

## Quick start (Using RPM or DEB)

  We have confirmed the operation on CentOS 7.6 and Ubuntu 18.04.

Note:
-  When you install this package, a gsadm OS user are created in the OS.  
  Execute the operating command as the gsadm user.  
- You don't need to set environment vatiable GS_HOME and GS_LOG.

### Install

    (CentOS)
    $ sudo rpm -ivh griddb_nosql-X.X.X-linux.x86_64.rpm

    (Ubuntu)
    $ sudo dpkg -i griddb_nosql-X.X.X_amd64.deb

    Note: X.X.X is the GridDB version.

### Start a server
    [gsadm]$ gs_passwd admin
      #input your_password
    [gsadm]$ vi conf/gs_cluster.json
      #    "clusterName":"your_clustername" #<-- input your_clustername
    [gsadm]$ export no_proxy=127.0.0.1
    [gsadm]$ gs_startnode
    [gsadm]$ gs_joincluster -c your_clustername -u admin/your_password

### Execute a sample program
    $ export CLASSPATH=${CLASSPATH}:/usr/share/java/gridstore.jar
    $ mkdir gsSample
    $ cp /usr/griddb-X.X.X/docs/sample/program/Sample1.java gsSample/.
    $ javac gsSample/Sample1.java
    $ java gsSample/Sample1 239.0.0.1 31999 your_clustername admin your_password
      --> Person:  name=name02 status=false count=2 lob=[65, 66, 67, 68, 69, 70, 71, 72, 73, 74]

### Stop a server
    [gsadm]$ gs_stopcluster -u admin/your_password
    [gsadm]$ gs_stopnode -u admin/your_password


## Document
  Refer to the file below for more detailed information.  
  - [GridDB Technical Design Document](https://griddb.github.io/griddb_nosql/manual/GridDBTechnicalDesignDocument.pdf)
  - [Quick Start Guide](https://griddb.github.io/griddb_nosql/manual/GridDB_QuickStartGuide.html)
  - [API Reference](https://griddb.github.io/griddb_nosql/manual/GridDB_API_Reference.html)
  - [RPM Installation Guide](https://griddb.github.io/griddb_nosql/manual/GridDB_RPM_InstallGuide.html)
  - [V3.0 Release Notes](docs/GridDB-3.0.0-CE-RELEASE_NOTES.md)
  - [V4.0 Release Notes](docs/GridDB-4.0-CE-RELEASE_NOTES.md)
  - [V4.1 Release Notes](docs/GridDB-4.1-CE-RELEASE_NOTES.md)
  - [V4.2 Release Notes](docs/GridDB-4.2-CE-RELEASE_NOTES.md)
  - [DEB Installation Guide](https://griddb.github.io/griddb_nosql/manual/GridDB_DEB_InstallGuide.html)

## Client and Connector
  There are other clients and API for GridDB.
  * [GridDB C Client](https://github.com/griddb/c_client)
  * [GridDB Python Client](https://github.com/griddb/python_client)
  * [GridDB Ruby Client](https://github.com/griddb/ruby_client)
  * [GridDB Go Client](https://github.com/griddb/go_client)
  * [GridDB Node.JS Client](https://github.com/griddb/nodejs_client)
  * [GridDB PHP Client](https://github.com/griddb/php_client)
  * [GridDB Perl Client](https://github.com/griddb/perl_client)
  * [GridDB WebAPI](https://github.com/griddb/webapi)

  There are some connectors for other OSS.
  * [GridDB connector for Apache Hadoop MapReduce](https://github.com/griddb/griddb_hadoop_mapreduce)
  * [GridDB connector for YCSB (https://github.com/brianfrankcooper/YCSB/tree/master/griddb)](https://github.com/brianfrankcooper/YCSB/tree/master/griddb)
  * [GridDB connector for KairosDB](https://github.com/griddb/griddb_kairosdb)
  * [GridDB connector for Apache Spark](https://github.com/griddb/griddb_spark)
  * [GridDB Foreign Data Wrapper for PostgreSQL (https://github.com/pgspider/griddb_fdw)](https://github.com/pgspider/griddb_fdw)
  * [GridDB Sample Application for Apache Kafka](https://github.com/griddb/griddb_kafka_sample_app)
  * [GridDB Data Source for Grafana](https://github.com/griddb/griddb-datasource)
  * [GridDB Plugin for Redash](https://github.com/griddb/griddb-redash)
  * [GridDB Plugin for Fluentd](https://github.com/griddb/fluent-plugin-griddb)

## Community
  * Issues  
    Use the GitHub issue function if you have any requests, questions, or bug reports. 
  * PullRequest  
    Use the GitHub pull request function if you want to contribute code.
    You'll need to agree GridDB Contributor License Agreement(CLA_rev1.1.pdf).
    By using the GitHub pull request function, you shall be deemed to have agreed to GridDB Contributor License Agreement.

## License
  The server source license is GNU Affero General Public License (AGPL), 
  while the Java client library license and the operational commands is Apache License, version 2.0.
  See 3rd_party/3rd_party.md for the source and license of the third party.

