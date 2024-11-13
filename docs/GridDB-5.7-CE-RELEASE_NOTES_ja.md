# GridDB CE 5.7

## 変更点

GridDB CE V5.7の主な変更点は以下のとおりです。

<運用強化>

- SQLメモリ使用量監視機能

    SQLを実行する際、GridDBは各種演算や処理のために作業領域用メモリを使用します。この作業用メモリの使用量を監視し、GridDBの安定稼働を妨げるほど過剰にメモリを必要とした場合にはSQLを自動的に停止させ、そのSQLをログに出力します。

- データベースリカバリ時間の短縮

    障害が発生した場合でも、それまでに登録されたデータを復元するために、GridDBはリカバリ処理を実行します。この処理を並列実行できるように機能強化しました。これにより従来よりもリカバリ時間が短縮され、大規模なデータに対して高速にノードが起動できるようになりました。

<APIの強化>

- WebAPIの強化

    従来、WebAPIでは、データ登録、検索等の一部データ操作のみをサポートしていました。今回、DDL、DMLの機能を追加し、WebAPIのみで一通りのデータ操作ができるようになりました。

- PythonAPI(SQL)ガイドの提供

    従来、Python向けにはNoSQL APIのみを提供していました。今回、OSSを利用してGridDBのSQLを利用するためのガイドを提供しました。

---

## SQLメモリ使用量監視機能

### 設定方法

ノード定義ファイル(gs_node.json)の以下のパラメータで設定します。

  * /sql/failOnTotalMemoryLimit：SQL処理用メモリの総上限を超えるメモリを必要とする場合にSQLの実行を強制停止するかどうかを設定します。デフォルト値はfalse。
  * /sql/totalMemoryLimit：SQL処理用メモリの総上限(ノード単位)。デフォルト値は0MB。

### メタテーブルの追加

また、以下のリソース消費統計値に関するメタテーブルを追加しました。
- 実行中のステートメント(SQL)のリソース統計： #statement_resources
- 実行中の分散タスクのリソース統計： #task_resources

## データベースリカバリ時間の短縮

### 設定方法

ノード定義ファイル(gs_node.json)の以下のパラメータでリカバリ処理の並列度を指定します。

  * /dataStore/recoveryConcurrency：デフォルトは/dataStore/concurrencyの設定値です。


## WebAPIの強化

以下をご参照ください。
https://github.com/griddb/webapi

## PythonAPI(SQL)ガイドの提供

以下をご参照ください。
https://github.com/griddb/jdbc/tree/master/sample
