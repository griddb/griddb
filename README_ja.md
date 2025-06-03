<img src="https://griddb.org/brand-resources/griddb-logo/png/color.png" align="center" height="240" alt="GridDB"/>

[![Visit Website](https://img.shields.io/badge/website-visit-orange.svg)](https://griddb.net) 
![GitHub All Releases](https://img.shields.io/github/downloads/griddb/griddb_nosql/total.svg) 
![GitHub release](https://img.shields.io/github/release/griddb/griddb_nosql.svg)
## 概要

GridDBは、NoSQLインタフェースとSQLインタフェースを兼ね備えたIoT向けデータベースです。

GridDBが提供する機能は『[GridDB 機能リファレンス](https://github.com/griddb/docs-ja/blob/master/manuals/md_reference_feature/md_reference_feature.md)』を参照ください。

本リポジトリにはサーバとJavaクライアントがあります。JDBCドライバは[jdbcリポジトリ](https://github.com/griddb/jdbc/blob/master/README_ja.md)にあります。

## クイックスタート (ソースコードの利用)

  以下のLinux(x64)環境での動作を確認しています。
  - Ubuntu 22.04(gcc 11), RockyLinux 9.4(gcc 11)

  ※事前にtclをインストールしてください。例) yum install tcl.x86_64  
  ※事前にPython3をインストールしてください。例) yum install python3

### サーバ、クライアント(java)のビルド

    $ ./bootstrap.sh
    $ ./configure
    $ make 
    
  ※JavaクライアントをMavenでビルドしたい場合は以下のコマンドを実行してください。gridstore-X.X.X.jarがtarget/の下に生成されます。 

    $ cd java_client
    $ ./make_source_for_mvn.sh
    $ mvn clean
    $ mvn package

### サーバの起動
    $ export GS_HOME=$PWD
    $ export GS_LOG=$PWD/log
    $ export PATH=${PATH}:$GS_HOME/bin

    $ gs_passwd admin
      #input your_password
    $ vi conf/gs_cluster.json
      #    "clusterName":"your_clustername" #<-- input your_clustername

    $ gs_startnode
    $ gs_joincluster -c your_clustername -u admin/your_password

### サンプルプログラムの実行
    $ export CLASSPATH=${CLASSPATH}:$GS_HOME/bin/gridstore.jar:.
    $ mkdir gsSample
    $ cp $GS_HOME/docs/sample/program/Sample1.java gsSample/.
    $ javac gsSample/Sample1.java
    $ java gsSample/Sample1 239.0.0.1 31999 your_clustername admin your_password
      --> Person:  name=name02 status=false count=2 lob=[65, 66, 67, 68, 69, 70, 71, 72, 73, 74]

### サーバの停止
    $ gs_stopcluster -u admin/your_password
    $ gs_stopnode -u admin/your_password

## [クイックスタート (GridDBサービスとCLIの利用)](docs/UsingServiceAndCLI.md)

## クイックスタート (RPM/DEBファイルの利用)

  以下のLinux(x64)環境での動作を確認しています。
  - Ubuntu 22.04、RockyLinux 9.4

  ※事前にPython3をインストールしてください。例) yum install python3

  - このパッケージをインストールすると、OS内にgsadmユーザが作成されます。運用コマンドはgsadmユーザで操作してください。  
  - gsadmユーザでログインすると環境変数 GS_HOMEとGS_LOGが自動的に設定されます。また、運用コマンドの場所が環境変数 PATHに設定されます。
  - Javaクライアントのライブラリ(gridstore.jar)は/usr/share/java上に、サンプルは/usr/griddb-XXX/docs/sample/program上に配置されます。
  - 過去版がインストールされている場合は、アンインストール後、/var/lib/gridstore上のconf/,data/を削除してください。

### インストール
    (CentOS/RockyLinux)
    $ sudo rpm -ivh griddb-X.X.X-linux.x86_64.rpm

    (Ubuntu)
    $ sudo dpkg -i griddb_X.X.X_amd64.deb

    ※ X.X.Xはバージョンを意味します。

### サーバの起動
    [gsadm]$ cp /usr/griddb-X.X.X/conf_multicast/* conf/.
    ※ デフォルトはローカル接続限定の設定になっていますので、コンフィグを変更してください。

    [gsadm]$ gs_passwd admin
      #input your_password
    [gsadm]$ vi conf/gs_cluster.json
      #    "clusterName":"your_clustername" #<-- input your_clustername
    [gsadm]$ gs_startnode
    [gsadm]$ gs_joincluster -c your_clustername -u admin/your_password

### サンプルプログラムの実行
    $ export CLASSPATH=${CLASSPATH}:/usr/share/java/gridstore.jar:.
    $ mkdir gsSample
    $ cp /usr/griddb-X.X.X/docs/sample/program/Sample1.java gsSample/.
    $ javac gsSample/Sample1.java
    $ java gsSample/Sample1 239.0.0.1 31999 your_clustername admin your_password
      --> Person:  name=name02 status=false count=2 lob=[65, 66, 67, 68, 69, 70, 71, 72, 73, 74]

### サーバの停止
    [gsadm]$ gs_stopcluster -u admin/your_password
    [gsadm]$ gs_stopnode -u admin/your_password

[インストール時のトラブルシューティング](docs/TroubleShootingTips_ja.md)もご参照ください。

## ドキュメント
  以下のドキュメントがあります。
  * [機能リファレンス](https://github.com/griddb/docs-ja/blob/master/manuals/md_reference_feature/md_reference_feature.md)
  * [クイックスタートアップガイド](https://github.com/griddb/docs-ja/blob/master/manuals/GridDB_QuickStartGuide/toc.md)
  * [Java APIリファレンス](http://griddb.github.io/docs-ja/manuals/md_reference_java_api/md_reference_java_api.html)
  * [C APIリファレンス](http://griddb.github.io/docs-ja/manuals/md_reference_c_api/md_reference_c_api.html)
  * [TQLリファレンス](https://github.com/griddb/docs-ja/blob/master/manuals/md_reference_tql/md_reference_tql.md)
  * [JDBCドライバ説明書](https://github.com/griddb/docs-ja/blob/master/manuals/md_reference_jdbc/md_reference_jdbc.md)
  * [SQLリファレンス](https://github.com/griddb/docs-ja/blob/master/manuals/md_reference_sql/md_reference_sql.md)
  * [SQLチューニングガイド](https://github.com/griddb/docs-ja/blob/master/manuals/md_sql_tuning_guide/md_sql_tuning_guide.md)
  * [プログラミングガイド](https://github.com/griddb/docs-ja/blob/master/manuals/md_programming_guide/md_programming_guide.md)
  * [V3.0 Release Notes](docs/GridDB-3.0.0-CE-RELEASE_NOTES_ja.md)
  * [V4.0 Release Notes](docs/GridDB-4.0-CE-RELEASE_NOTES_ja.md)
  * [V4.1 Release Notes](docs/GridDB-4.1-CE-RELEASE_NOTES_ja.md)
  * [V4.2 Release Notes](docs/GridDB-4.2-CE-RELEASE_NOTES_ja.md)
  * [V4.3 Release Notes](docs/GridDB-4.3-CE-RELEASE_NOTES_ja.md)
  * [V4.5 Release Notes](docs/GridDB-4.5-CE-RELEASE_NOTES_ja.md)
  * [V4.6 Release Notes](docs/GridDB-4.6-CE-RELEASE_NOTES_ja.md)
  * [V5.0 Release Notes](docs/GridDB-5.0-CE-RELEASE_NOTES_ja.md)
  * [V5.1 Release Notes](docs/GridDB-5.1-CE-RELEASE_NOTES_ja.md)
  * [V5.3 Release Notes](docs/GridDB-5.3-CE-RELEASE_NOTES_ja.md)
  * [V5.5 Release Notes](docs/GridDB-5.5-CE-RELEASE_NOTES_ja.md)
  * [V5.6 Release Notes](docs/GridDB-5.6-CE-RELEASE_NOTES_ja.md)
  * [V5.7 Release Notes](docs/GridDB-5.7-CE-RELEASE_NOTES_ja.md)
  * [V5.8 Release Notes](docs/GridDB-5.7-CE-RELEASE_NOTES_ja.md)

## クライアントとコネクタ
  Java以外のクライアント、APIもあります。
  
  (NoSQL Interface)
  * [GridDB C Client](https://github.com/griddb/c_client/blob/master/README_ja.md)
  * [GridDB Python Client](https://github.com/griddb/python_client)
  * [GridDB Ruby Client](https://github.com/griddb/ruby_client)
  * [GridDB Go Client](https://github.com/griddb/go_client)
  * [GridDB Node.JS Client (SWIG based)](https://github.com/griddb/nodejs_client)
  * [GridDB Node API (node-addon-api based)](https://github.com/griddb/node-api)
  * [GridDB PHP Client](https://github.com/griddb/php_client)
  * [GridDB Perl Client](https://github.com/griddb/perl_client)
  * [GridDB Rust Client](https://github.com/griddb/rust_client)
  
  (SQL Interface)
  * [GridDB JDBC Driver](https://github.com/griddb/jdbc)
  
  (NoSQL & SQL Interface)
  * [GridDB WebAPI](https://github.com/griddb/webapi)
  * [GridDB CLI](https://github.com/griddb/cli)
  
  (その他)
  * [GridDB Export/Import](https://github.com/griddb/expimp)

  他のOSSと接続するためのコネクタもあります。
  * [GridDB connector for Apache Hadoop MapReduce](https://github.com/griddb/griddb_hadoop_mapreduce/blob/master/README_ja.md)
  * [GridDB connector for YCSB (https://github.com/brianfrankcooper/YCSB/tree/master/griddb)](https://github.com/brianfrankcooper/YCSB/tree/master/griddb)
  * [GridDB connector for KairosDB](https://github.com/griddb/griddb_kairosdb)
  * [GridDB connector for Apache Spark](https://github.com/griddb/griddb_spark)
  * [GridDB Foreign Data Wrapper for PostgreSQL (https://github.com/pgspider/griddb_fdw)](https://github.com/pgspider/griddb_fdw)
  * [GridDB Data Source for Grafana](https://github.com/griddb/griddb-datasource)
  * [GridDB Plugin for Redash](https://github.com/griddb/griddb-redash)
  * [GridDB Plugin for Fluentd](https://github.com/griddb/fluent-plugin-griddb)
  * [GridDB Plugin for Tableau](https://github.com/griddb/tableau-plugin-griddb)
  * [GridDB connector for Apache Kafka](https://github.com/griddb/griddb-kafka-connect)

## [パッケージ](docs/Packages.md)

## コミュニティ
  * Issues  
    質問、不具合報告はissue機能をご利用ください。
  * PullRequest  
    GridDB Contributor License Agreement(CLA_rev1.1.pdf)に同意して頂く必要があります。
    PullRequest機能をご利用の場合はGridDB Contributor License Agreementに同意したものとみなします。

## ライセンス
  サーバソースのライセンスはGNU Affero General Public License (AGPL)、
  Javaクライアントと運用コマンドのライセンスはApache License, version 2.0です。
  サードパーティのソースとライセンスについては3rd_party/3rd_party.mdを参照ください。
