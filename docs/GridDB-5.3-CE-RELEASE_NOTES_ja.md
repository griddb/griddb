# GridDB CE 5.3

## 変更点

GridDB CE V5.3の主な変更点は以下のとおりです。

V5.3では、精度指定付TIMESTMP型を追加し、より高精度の時刻表現を可能にしました。また、時系列データの集計演算や補間演算に関する機能を新たに追加しました。

- 時系列データ管理・検索機能の強化

    - 高精度の時刻表現を行うための、精度指定値付TIMESTAMP型の追加

    データベースの型として、精度指定付TIMESTMP型を追加しました。従来、日時を表現するTIMESTAMP型はミリ秒精度でしたが、より高精度のマイクロ秒精度やナノ秒精度の日時表現が可能になりました。 (本機能は、Javaクライアント, Cクライアント, JDBCドライバで利用可能です。)

    - 時系列データなど時刻値を持つデータに対する集計演算および補間演算の追加(SQL)

    時刻値を持つデータに対して一定時間間隔毎のデータ集合の集計演算や補間演算を行えるようになりました。データを一定の時刻間隔毎の集合に分割し、各集合の値に対して集計演算や補間演算を行います。SQLに専用の構文(GROUP BY RANGE)を追加しています。これにより、大量の時系列データの集計をSQLを用いて行えるようになり、従来よりも簡単かつ高速に行えるようになります。(本機能は、JDBCドライバで利用可能です。)
 
---

## 精度指定値付TIMESTAMP型

### 精度指定値付TIMESTAMP型の利用方法

精度指定値付TIMESTAMP型を利用して、コンテナ生成、データ登録、データ参照を行う
以下のサンプルコードをご参考願います。

- Javaクライアント: NanoTimestamp.java
- Cクライアント: NanoTimestamp.c
- JDBCドライバ: NanoTimestamp-SQL.md

※ コレクションコンテナのロウキーは3種類の精度のTIMESTAMP型に対応していますが、時系列コンテナのロウキーはミリ秒精度のTIMESTAMP型に限定されます。

## 時系列データなど時刻値を持つデータに対する集計演算および補間演算

### 構文

SQLに専用の構文(GROUP BY RANGE)を追加しました。

``` example
SELECT <expr_list> FROM <table_list> WHERE <range_cond>
GROUP BY RANGE(<key>) EVERY(<interval>, <unit>[, <offset>]) [FILL(<fill_opt>)]
```

- key: 集計・補間の基準として用いるカラムの名前。TIMESTAMP型(精度は任意)のカラムのみ記述可能である。
- interval: 集計・補間間隔を示す整数値。
- unit: 集計・補間間隔を示す値の単位。DAY、HOUR、MINUTE、SECOND、MILLISECOND
- offset: 集計・補間間隔の開始位置オフセット。
- fill_opt: 集計・補間対象のグループのうち、グループ内に演算対象の入力ロウが1件も存在しない場合の、各カラム値の補間方法の指定。LINEAR(前後カラムで線形補間)、NONE(ロウ出力なし)、NULL(NULL埋め)、PREVIOUS(前方のカラムと同値)

※ expr_listに指定できる集計関数: AVG, COUNT, MAX, MIN, SUM, TOTAL, GROUP_CONCAT, STDDEV, MEDIAN, PERCENTILE_CONTなど。

### 集計演算のSQLの例

``` example
SELECT key,avg(value) FROM table1 
WHERE key BETWEEN TIMESTAMP('2023-01-01T00:00:00Z') AND TIMESTAMP('2023-01-01T00:01:00Z')
GROUP BY RANGE (key) EVERY (20, SECOND)
```

### 補間演算のSQLの例

``` example
SELECT * FROM table1 
WHERE key BETWEEN TIMESTAMP('2023-01-01T00:00:00Z') AND TIMESTAMP('2023-01-01T00:01:00Z')
GROUP BY RANGE (key) EVERY (10, SECOND) FILL (LINEAR)
```
