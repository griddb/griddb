# インストール時のトラブルシューティング

## 起動できない場合

1. 管理ユーザ（例：admin）のパスワードが設定されているか確認してください。  

    http://griddb.org/griddb_nosql/manual/GridDB_QuickStartGuide_ja.html#sec-2-2-4 を参照してください。

2. gs_cluster.jsonファイルにクラスタ名が設定されているか確認してください。

    http://griddb.org/griddb_nosql/manual/GridDB_QuickStartGuide_ja.html#sec-2-3-2 を参照してください。

3. 「$ hostname -i」にてホスト名から127.0.0.1以外のIPアドレスが取得できるか確認してください。  
    - 取得できない場合は、/etc/hostsファイルの設定を確認してください。  
    
    http://griddb.org/griddb_nosql/manual/GridDB_QuickStartGuide_ja.html#sec-2-3-1 を参照してください。

4. RPM/DEBパッケージでインストール後、運用コマンドの実行で環境変数が未設定のエラーが発生した場合
    - OSユーザ gsadmでログインしていない可能性があります。gsadmのパスワードを設定した上でgsadmでログインしてください。環境変数が自動設定されます。
    - もしsuコマンドでログインする場合は、「$ su - gsadm」 のように「-」もしくは「-l」オプションを付けてください。

    http://griddb.org/griddb_nosql/manual/GridDB_RPM_InstallGuide.html#sec-1.3 を参照してください。

## 運用コマンドの操作ができない場合

5. プロキシ変数(http_proxy)が設定されている場合には、プロキシ側にアクセスしないようにする必要があります。
    - 「$ export no_proxy=127.0.0.1,10.0.2.15」のように、127.0.0.1と「$ hostname -i」で出力されるIPアドレスを設定してください。  

    http://griddb.org/griddb_nosql/manual/GridDB_QuickStartGuide_ja.html#sec-3 の【運用コマンドを利用する上での注意点】を参照してください。

## (Javaなど)クライアント操作ができない場合

6. ファイアウォールが原因かもしれません。ファイアウォールに通信用ポートNoを許可してみてください。
    - CentOSの場合の例： $ firewall-cmd --zone=public --add-port=31999/udp
    - Ubuntuの場合の例： $ ufw allow 31999/udp

    http://griddb.org/griddb_nosql/manual/GridDB_QuickStartGuide_ja.html#sec-5-1-2 の"/transaction/notificationPort"を参照してください。

## (AWS、Azureのクラウド環境のように)マルチキャストが利用できない環境の場合

7. デフォルトのマルチキャスト方式ではなく、固定リスト方式もしくはプロバイダ方式のクラスタ構成を使ってください。
    http://griddb.org/griddb_nosql/manual/ja/GridDB_FeaturesReference.html#%E3%82%AF%E3%83%A9%E3%82%B9%E3%82%BF%E6%A7%8B%E6%88%90%E6%96%B9%E5%BC%8F を参照してください。
