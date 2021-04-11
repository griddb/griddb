<img src="https://griddb.org/brand-resources/griddb-logo/png/color.png" align="center" height="240" alt="GridDB"/>

[![Visit Website](https://img.shields.io/badge/website-visit-orange.svg)](https://griddb.net)
![GitHub All Releases](https://img.shields.io/github/downloads/griddb/griddb_nosql/total.svg)
![GitHub release](https://img.shields.io/github/release/griddb/griddb_nosql.svg)
## Overview
  GridDB is Database for IoT with both NoSQL interface and SQL Interface.

  Please refer to [GridDB Features Reference](https://github.com/griddb/docs-en/blob/master/manuals/GridDB_FeaturesReference.md) for functionality.

  This repository includes server and Java client. And [jdbc repository](https://github.com/griddb/jdbc) includes JDBC Driver.

## Quick start (Using source code)
  We have confirmed the operation on CentOS 7.6 (gcc 4.8.5), Ubuntu 18.04 (gcc 4.8.5) and openSUSE Leap 15.1 (gcc 4.8.5).

  Note: Please install tcl like "yum install tcl.x86_64" in advance.

### Build a server and client(Java)
    $ ./bootstrap.sh
    $ ./configure
    $ make

  Note: When you use maven build for Java client, please run the following command. Then gridstore-X.X.X.jar file is created on target/.  

    $ cd java_client
    $ ./make_source_for_mvn.sh
    $ mvn clean
    $ mvn install

### Start a server
    $ export GS_HOME=$PWD
    $ export GS_LOG=$PWD/log
    $ export PATH=${PATH}:$GS_HOME/bin

    $ bin/gs_passwd admin
      #input your_password
    $ vi conf/gs_cluster.json
      #    "clusterName":"your_clustername" #<-- input your_clustername

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

  We have confirmed the operation on CentOS 7.8/8.1, Ubuntu 18.04 and openSUSE Leap 15.1.

Note:
- When you install this package, a gsadm OS user are created in the OS.  
  Execute the operating command as the gsadm user.  
- You don't need to set environment vatiable GS_HOME and GS_LOG.
- There is Java client library (gridstore.jar) on /usr/share/java and a sample on /usr/gridb-XXX/docs/sample/programs.
- The packages don't include trigger function.
- Please install Python2 in advance except CentOS7.

### Install

    (CentOS)
    $ sudo rpm -ivh griddb-X.X.X-linux.x86_64.rpm

    (Ubuntu)
    $ sudo dpkg -i griddb_X.X.X_amd64.deb

    (openSUSE)
    $ sudo rpm -ivh griddb-X.X.X-opensuse.x86_64.rpm

    Note: X.X.X is the GridDB version.

### Start a server
    [gsadm]$ gs_passwd admin
      #input your_password
    [gsadm]$ vi conf/gs_cluster.json
      #    "clusterName":"your_clustername" #<-- input your_clustername
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

If necessary, please refer to [Installation Troubleshooting](docs/TroubleShootingTips.md).

## Document
  Refer to the file below for more detailed information.  
  - [Features Reference](https://github.com/griddb/docs-en/blob/master/manuals/GridDB_FeaturesReference.md)
  - [Quick Start Guide](https://github.com/griddb/docs-en/blob/master/manuals/GridDB_QuickStartGuide.md)
  - [Java API Reference](http://griddb.github.io/docs-en/manuals/GridDB_Java_API_Reference.html)
  - [C API Reference](http://griddb.github.io/docs-en/manuals/GridDB_C_API_Reference.html)
  - [TQL Reference](https://github.com/griddb/docs-en/blob/master/manuals/GridDB_TQL_Reference.md)
  - [JDBC Driver UserGuide](https://github.com/griddb/docs-en/blob/master/manuals/GridDB_JDBC_Driver_UserGuide.md)
  - [SQL Reference](https://github.com/griddb/docs-en/blob/master/manuals/GridDB_SQL_Reference.md)
  - [V3.0 Release Notes](docs/GridDB-3.0.0-CE-RELEASE_NOTES.md)
  - [V4.0 Release Notes](docs/GridDB-4.0-CE-RELEASE_NOTES.md)
  - [V4.1 Release Notes](docs/GridDB-4.1-CE-RELEASE_NOTES.md)
  - [V4.2 Release Notes](docs/GridDB-4.2-CE-RELEASE_NOTES.md)
  - [V4.3 Release Notes](docs/GridDB-4.3-CE-RELEASE_NOTES.md)
  - [V4.5 Release Notes](docs/GridDB-4.5-CE-RELEASE_NOTES.md)
  - [V4.6 Release Notes](docs/GridDB-4.6-CE-RELEASE_NOTES.md)

## Client and Connector
  There are other clients and API for GridDB.
  
  (NoSQL Interface)
  * [GridDB C Client](https://github.com/griddb/c_client)
  * [GridDB Python Client](https://github.com/griddb/python_client)
  * [GridDB Ruby Client](https://github.com/griddb/ruby_client)
  * [GridDB Go Client](https://github.com/griddb/go_client)
  * [GridDB Node.JS Client (SWIG based)](https://github.com/griddb/nodejs_client)
  * [GridDB Node API (node-addon-api based)](https://github.com/griddb/node-api)
  * [GridDB PHP Client](https://github.com/griddb/php_client)
  * [GridDB Perl Client](https://github.com/griddb/perl_client)
  
  (SQL Interface)
  * [GridDB JDBC Driver](https://github.com/griddb/jdbc)
  
  (NoSQL & SQL Interface)
  * [GridDB WebAPI](https://github.com/griddb/webapi)
  * [GridDB CLI](https://github.com/griddb/cli)

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
  * [GridDB Plugin for Tableau](https://github.com/griddb/tableau-plugin-griddb)

## [Packages](docs/Packages.md)

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
