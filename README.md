## Overview
  GridDB has a KVS (Key-Value Store)-type data model that is suitable for sensor data stored in a timeseries. It is a database that can be easily scaled-out according to the number of sensors.

  * High Reliability  
    It is equipped with a structure to spread out the replication of key value data among fellow nodes so that in the event of a node failure, automatic failover can be carried out in a matter of seconds by using the replication function of other nodes.

  * High Performance (in-memory)  
   Even if the memory is increased to handle big data in a RDB, it is known that CPU power cannot be fully exploited and only around 10% of CPU resources are allocated to actual data processing due to the large overhead in buffer management, etc. Given the increase in memory size, GridDB minimizes the amount of overheads required previously by lightening the buffer process and recovery process, and by making it lock-free during data processing.

  * Advanced data model and operation model  
    In a traditional distributed KVS, data is handled using operations such as Put/Get/Remove. GridDB expands these functions greatly to support the definition function for organizational data, SQL-like query function, transaction function and Java API (Application Programming Interface) so that RDB users are able to introduce the system smoothly. The key value represents the data in a set of records known as a key container. This is similar to the relationship between a RDB table name and table. It is also equipped with an application function for sensor data management.

  This repository includes server and Java client.

## Quick start
### Build a server and client(Java)
    We have confirmed the operation on CentOS 6.7 and gcc version 4.4.7.

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

## Document
  Refer to the file below for more detailed information.  
  The documents below are stored in the docs folder.
  - [GridDB Technical Design Document](https://griddb.github.io/griddb_nosql/manual/GridDBTechnicalDesignDocument.pdf)  (manual/GridDBTechnicalDesignDocument.pdf)
  - [Quick Start Guide](https://griddb.github.io/griddb_nosql/manual/GridDB_QuickStartGuide.html) (manual/GridDB_QuickStartGuide.html)
  - [API Reference](https://griddb.github.io/griddb_nosql/manual/GridDB_API_Reference.html) (manual/GridDB_API_Reference.html)
  - [RPM Installation Guide](https://griddb.github.io/griddb_nosql/manual/GridDB_RPM_InstallGuide.html) (manual/GridDB_RPM_InstallGuide.html)
  - [V3.0 Release Notes](docs/GridDB-3.0.0-CE-RELEASE_NOTES.md) (GridDB-3.0.0-CE-RELEASE_NOTES.md)

## Client and Connector
  There are other clients for GridDB.
  * [GridDB C Client](https://github.com/griddb/c_client)
  * [GridDB Python Client](https://github.com/griddb/python_client)
  * [GridDB Ruby Client](https://github.com/griddb/griddb_client)

  There are some connectors for other OSS.
  * [GridDB connector for Apache Hadoop MapReduce](https://github.com/griddb/griddb_hadoop_mapreduce)
  * [GridDB connector for YCSB](https://github.com/griddb/griddb_ycsb)
  * [GridDB connector for KairosDB](https://github.com/griddb/griddb_kairosdb)
  * [GridDB connector for Apache Spark](https://github.com/griddb/griddb_spark)
  * [GridDB Foreign Data Wrapper for PostgreSQL (https://github.com/pgspider/griddb_fdw)](https://github.com/pgspider/griddb_fdw)

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

