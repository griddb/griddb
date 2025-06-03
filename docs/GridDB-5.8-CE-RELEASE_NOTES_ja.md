# GridDB CE 5.8

## 変更点

GridDB CE V5.8の主な変更点は以下のとおりです。

- SQL最適化の強化

    SQL検索実行時におけるジョイン方向（駆動表・内部表）の最適化を強化しました。従来のSQLプランでは、ジョイン演算の方向はユーザが記述したSELECT文の内容や順序に基づき、ルールを用いて決定されていました。そのため、ユーザはルールを理解し、テーブル規模や絞込み条件に応じて適切なプランが生成されるようにSELECT文を記述する必要がありました。今回の強化により、検索実行中に算出されるロウ数（テーブル規模や絞込み条件を考慮した概算値）をコストとして計算し、このコストを基にジョイン方向を最適化したプランを生成できるようになりました。

- 時系列データ管理・検索機能の強化

    SQL分析関数機能強化(移動平均演算)：SQL実行機能における分析関数のオプション構文として、SQL標準で定義されているフレーム句(分析対象とするデータ範囲の件数・幅指定)に対応しました。これにより、SQLを用いた時系列データのトレンド分析が容易に実現できるようになりました。

    日付演算機能の精度向上：現状のSQLによる日時型演算機能の一部で行えなかった、マイクロ秒・ナノ秒精度の演算が可能となりました。

---

## SQL最適化の強化

結合処理において、最初にアクセスするテーブルを駆動表、次にアクセスして結合するテーブルを内部表といいます。駆動表の決め方には、コストベースによる方法とルールベースによる方法の２種類あります。いずれを用いるかは以下を使って設定できます。
- クラスタ定義ファイル(gs_cluster.json)
- ヒント

クラスタ定義ファイル(gs_cluster.json)を使う場合：

  * /sql/costBasedJoinDriving：SQLプラン生成の際、ジョイン方向（駆動表・内部表）の決定にコストベースによる方法を用いるかどうかを指定します。用いない(false)場合は、ルールベースを用いて駆動表を決定します。デフォルト値はコストベースによる方法(true)です。

ヒントを使う場合：

  * CostBasedJoinDriving()：コストベースによる方法で駆動表を決定します。
  * NoCostBasedJoinDriving()：コストベースによる方法を用いないで、ルールベースによる方法で駆動表を決定します。

## 時系列データ管理・検索機能の強化

### SQL分析関数機能強化(移動平均演算)

分析関数にFRAME句が追加されました。

FRAME句はパーティション全体ではなくフレーム上で作動するウィンドウ関数に対して、ウィンドウフレームを構成する行の集合を指定します。

``` example
    関数 OVER ( [PARTITION BY 式1 ] [ ORDER BY 式2 ] [ FRAME句 ] )
```

ここで、FRAME句の構文は以下の通りです。

``` example
    ROWS | RANGE <フレーム開始値> | BETWEEN <フレーム開始値> AND <フレーム終了値>
```

フレーム開始値・終了値の構文は以下の通りです。 

``` example
    UNBOUNDED PRECEDING | UNBOUNDED FOLLOWING | CURRENT ROW | <フレーム境界値1> PRECEDING | <フレーム境界値2> FOLLOWING 
```

- CURRENT ROW: 分析対象の現在位置のロウを示します
- UNBOUNDED: パーティションの先頭もしくは末端を示します
- PRECEDING/FOLLOWING: 前方もしくは後方を示します

フレーム境界値の構文は以下の通りです。

``` example
    値1 | ( 値2, 単位 )
```

- 同名の集計関数と対応づく既存の分析関数すべてで、FRAME句を指定できます。

    AVG、COUNT、MAX、MIN、SUM、TOTAL、STDDEV、VAR

- 同名の集計関数と対応付かない以下の分析関数では、FRAME句を指定できません。

    ROW_NUMBER、LAG、LEAD

例)

各ロウの10件前までを集計対象とする移動平均を求める。

``` example
SELECT
    AVG(value1)
        OVER(ORDER BY time) 
        ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
FROM tbl1;
```
各ロウの10分前までを集計対象とする移動平均を求める。

``` example
SELECT
    AVG(value1)
        OVER(ORDER BY time)
        RANGE BETWEEN (10, MINUTE) PRECEDING AND CURRENT ROW
FROM tbl1;
```

### 日付演算機能の精度向上

SQLおよびTQLに以下の関数を追加しました。

- SQL
``` example
  * TIMESTAMP_MS(timestamp_string [, timezone])：時刻の文字列表現timestamp_stringの値を、ミリ秒精度のTIMESTAMP(3)型に変換します。
  * TIMESTAMP_US(timestamp_string [, timezone])：時刻の文字列表現timestamp_stringの値を、マイクロ秒精度のTIMESTAMP(6)型に変換します。
  * TIMESTAMP_NS(timestamp_string [, timezone])：時刻の文字列表現timestamp_stringの値を、ナノ秒精度のTIMESTAMP(9)型に変換します。
```

- TQL
``` example
  * TIMESTAMP_MS(str)：時刻の文字列表現をミリ秒精度のTIMESTAMP(3)型に変換します。
  * TIMESTAMP_US(str)：時刻の文字列表現をマイクロ秒精度のTIMESTAMP(6)型に変換します。
  * TIMESTAMP_NS(str)：時刻の文字列表現をナノ秒精度のTIMESTAMP(9)型に変換します。
```

また、以下の関数でマイクロ秒・ナノ秒精度の演算が可能となりました。

- SQL
``` example
  * TIMESTAMP_ADD(time_unit, timestamp, duration [, timezone])/TIMESTAMP_DIFF(time_unit, timestamp1, timestamp2 [, timezone])
```

- TQL
``` example
  * TIMESTAMPADD(time_unit, timestamp, duration)/TIMESTAMPDIFF(time_unit, timestamp1, timestamp2)
  ※ SQLの関数と同名のTIMESTAMP_ADD()/TIMESTAMP_DIFF()も追加しました。
```

