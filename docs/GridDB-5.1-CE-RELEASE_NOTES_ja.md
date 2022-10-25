# GridDB CE 5.1

## 変更点

GridDB CE V5.1の主な変更点は以下のとおりです。

- クライアント通信経路設定

    - クライアントを接続する際の通信経路を設定できます。これにより、クライアントをクラスタとは別ネットワークにも配置できるようになり、従来からのクラスタと同じネットワークからのクライアント接続、およびネットワーク的に離れた場所からのクライアント接続を共存して利用できます。 (本機能は、Javaクライアント, JDBCドライバでサポートされます。)

---

## クライアント通信経路設定

GridDBクラスタはクライアントに対する複数の通信経路を設定することができます。デフォルトのクラスタ-クライアント間の通信経路は、クラスタノード間の通信経路と共通のものとなりますが、複数通信経路設定を行うと、これとは別の通信経路を用いた接続が可能となります。この通信経路を外部通信経路と呼びます。クライアントはどちらの通信経路を利用するかを個別に指定することが可能となります。

※これまでは、サーバ側の設定により内部経路通信もしくは外部通信経路のいずれかの単一通信経路に限定されてしまい、クライアント側でどちらの通信経路を利用するかを個別に選択できませんでした。

### 設定方法

#### GridDBクラスタ(サーバ側)の複数通信経路設定

複数通信経路を有効にするには、ノード定義ファイル、クラスタ定義ファイルについて、
外部通信経路のIPアドレスを指定してクラスタを構成します。

クラスタを構成する各ノードにおけるノード定義ファイル(gs_node.json)で
外部通信経路のIPアドレスを指定してクラスタを構成します。

  * /transaction/publicServiceAddress : トランザクションサービスの外部通信経路に対応するIPアドレスを指定します。
  * /sql/publicServiceAddress : SQLサービスの外部通信経路に対応するIPアドレスを指定します。

※ /transaction/localServiceAddress, /sql/localServiceAddressは廃止されました。

設定例は以下のとおりです。

``` example
{
                 :
                 :
    "transaction":{
        "serviceAddress":"172.17.0.44",
        "publicServiceAddress":"10.45.1.10",        
        "servicePort":10001
    },      
    "sql":{
      "serviceAddress":"172.17.0.44",
      "publicServiceAddress":"10.45.1.10",      
      "servicePort":20001
    },
                 :
                 : 
```

また、クラスタ定義ファイル(gs_cluster.json)では
固定リスト方式の設定(/cluster/notificatioMember)に
外部通信経路のIPアドレスとポート番号を指定してクラスタを構成します。

  * /transactionPublic : 外部通信経路となるトランザクションサービス通信用のIPアドレス、ポート番号を指定します。
  * /sqlPublic : 外部通信経路となるSQLサービス通信用のIPアドレス、ポート番号を指定します。

設定例は以下のとおりです。

``` example
{
                             :
                             :
    "cluster":{
        "clusterName":"yourClusterName",
        "replicationNum":2,
        "heartbeatInterval":"5s",
        "loadbalanceCheckInterval":"180s",
        "notificationMember": [
            {
                "cluster": {"address":"172.17.0.44", "port":10010},
                "sync": {"address":"172.17.0.44", "port":10020},
                "system": {"address":"172.17.0.44", "port":10040},
                "transaction": {"address":"172.17.0.44", "port":10001},
                "sql": {"address":"172.17.0.44", "port":20001},
                "transactionPublic": {"address":"10.45.1.10", "port":10001},
                "sqlPublic": {"address":"10.45.1.10", "port":20001}
            }
        ]
    },
                             :
                             :
}
```

#### クライアント側の使い方

GridDBクラスタ(サーバ側)が複数通信経路設定がなされている場合に通信経路を選択可能です。
- 外部通信経路を選択する場合 : connectionRouteプロパティに"PUBLIC"を指定してください。
- 内部通信経路を選択する場合 : デフォルトで利用可能です。

プロパティの設定例は以下のとおりです。

(Javaクライアント)
```Java
Properies prop = new Properties();
props.setProperty("notificationMember", "10.45.1.10:10001");
props.setProperty("connectionRoute", "PUBLIC");
...
GridStore store = GridStoreFactory.getInstance().getGridStore(prop);
```

(JDBCドライバ)
```
url = "jdbc:gs:///yourClusterName/?notificationMember=10.45.1.10:20001&connectionRoute=PUBLIC"
```
