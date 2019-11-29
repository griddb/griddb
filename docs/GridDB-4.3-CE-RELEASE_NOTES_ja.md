# GridDB CE 4.3

## 変更点

GridDB CE V4.3の主な変更点は以下のとおりです。

#### スケールアップ強化
1. ブロックサイズ拡大
    - データベース初期作成時に選択できるブロックサイズを最大32MBまで拡張しました。ブロックサイズを大きく設定することで、ノード1台当たりで管理可能なデータ量が増加します。また、大量データのスキャンの性能改善が期待できます。
2. チェックポイントファイルの分割配置
    - チェックポイントファイルを分割し、複数ディレクトリへ分散配置できます。分散配置することでノード1台当たりで管理可能なデータ量が増加します。また、ディスク負荷の分散により、性能改善が期待できます。
3. 外部と内部の通信を分離するネットワーク構成
    - クライアント-ノード間の外部通信とノード間の内部通信にそれぞれ異なるネットワークインタフェースを割り当てることで、外部と内部の通信を分離できます。ネットワーク負荷の分散が可能になります。

#### 性能向上
4. 複合索引
    - 複数カラムを指定して索引を作成できます。複数カラムの値を組み合わせると選択性が高くなる場合に用いると性能改善が期待できます。

#### 開発機能
5. 複合ロウキー
    - 複数カラムを指定してロウキーを設定できます。複数カラムの値を組み合わせると値が一意になる場合に設定します。サロゲートキー(代替キー)が不要となるため、データ設計が容易になります。
6. タイムゾーン指定
    - 時刻文字列に「±hh:mm」形式でタイムゾーンを指定できます。

---

### 1. ブロックサイズ拡大

ブロックのサイズは64KB、1MB、4MB、8MB、16MB、32MBから選択可能になりました。デフォルトは64KBで、通常はデフォルト値のままで変更不要です。

ブロックのサイズは、クラスタ定義ファイル(gs_cluster.json)のパラメータ /dataStore/storeBlockSizeで設定します。

### 2. チェックポイントファイルの分割配置

チェックポイントファイルを分割し、複数ディレクトリへ分散配置できます。

ノード定義ファイル(gs_node.json)のパラメータで分割数と分割されたファイルの配置ディレクトリを設定します。
- /dataStore/dbFileSplitCount：分割数
- /dataStore/dbFilePathList：チェックポイントファイル分割時の分割チェックポイントファイルの配置ディレクトリリスト

設定例

    ```    
	"dataStore":{
        "dbFileSplitCount": 2,
        "dbFilePathList": ["/stg01", "/stg02"],
    ```    

### 3. 外部と内部の通信を分離するネットワーク構成

GridDBノードが行う通信のうち、トランザクション処理の通信は、クライアント-ノード間のクライアント通信(外部通信)と、ノード間のクラスタ内部通信、の2種類の通信経路を持ちます。

これまで、その2つは同じネットワークインタフェースを介して通信を行っていましたが、今回2つのネットワークを分離することが可能になりました。


ノード定義ファイル(gs_node.json)のパラメータで設定します。
- /transaction/serviceAddress: クライアント通信向けトランザクション処理用のアドレス
- /transaction/localServiceAddress(新規): クラスタ内部通信向けトランザクション処理用のアドレス

ノード定義ファイルの設定例は以下のとおりです。

    ```    
    "cluster":{
        "serviceAddress":"192.168.10.11",
        "servicePort":10010
    },
    "sync":{
        "serviceAddress":"192.168.10.11",
        "servicePort":10020
    },
    "system":{
        "serviceAddress":"192.168.10.11",
        "servicePort":10040,
              :
    },
    "transaction":{
        "serviceAddress":"172.17.0.11",
        "localServiceAddress":"192.168.10.11",
        "servicePort":10001,
              :
    },

    ```

### 4. 複合索引

ツリー索引について、複数のカラムを指定した索引を作成できます。これを複合索引と呼びます。

Javaクライアントを使う場合、IndexInfoクラスの以下のメソッドを使って、カラム名一覧（もしくはカラム番号一覧）を設定・取得します。
- void setColumnName(java.lang.String columnName)
- void setColumnList(java.util.List<java.lang.Integer> columns)
- java.util.List<java.lang.String> getColumnNameList()
- java.util.List<java.lang.Integer> getColumnList()

サンプルプログラム[CreateIndex.java](https://github.com/griddb/griddb_nosql/blob/master/sample/guide/ja/CreateIndex.java)の(3)をご参照ください。

なお、Cクライアントを使う場合は、サンプルプログラム[CreateIndex.c](https://github.com/griddb/c_client/blob/master/sample/guide/ja/CreateIndex.c)のcompositeInfo部分をご参照ください。

### 5. 複合ロウキー

コンテナタイプがコレクションの場合、ROWKEY(PRIMARY KEY)は先頭カラムより連続した複数のカラムに設定できます。ロウキーを複数のカラムに設定した場合は、複合ロウキーと呼びます。 

Javaクライアントを使う場合、ContainerInfoクラスの以下のメソッドを使って、複合ロウキーを設定します。
- void setRowKeyColumnList(java.util.List<java.lang.Integer> rowKeyColumnList)

例：  
    containerInfo.setRowKeyColumnList(Arrays.asList(0, 1));

サンプルプログラム[CompositeKeyMultiGet.java](https://github.com/griddb/griddb_nosql/blob/master/sample/guide/ja/CompositeKeyMultiGet.java)のbuildContainerInfo()部分をご参照ください。

なお、Cクライアントを使う場合は、サンプルプログラム[CompositeKeyMultiGet.c](https://github.com/griddb/c_client/blob/master/sample/guide/ja/CompositeKeyMultiGet.c)のrowKeyColumnList部分をご参照ください。

### 6. タイムゾーン指定

時刻文字列に「±hh:mm」形式でタイムゾーンを指定できます。

また、JavaクライアントのTimestampUtilsのメソッドにタイムゾーンの引数を追加します。
- add(timestamp, amount, timeUnit, zone)
- format(timestamp, zone)
- getFormat(zone)

