
## ビルド・実行方法

    $ export CLASSPATH=$GS_HOME/bin/gridstore.jar:./opencsv-3.9.jar:.
    $ javac pvrms/*.java
    $ java pvrms/SimplePv0 239.0.0.1 31999 your_clustername admin your_password
    $ java pvrms/SimplePv1 239.0.0.1 31999 your_clustername admin your_password
    $ java pvrms/SimplePv2 239.0.0.1 31999 your_clustername admin your_password
    $ java pvrms/SimplePv3 239.0.0.1 31999 your_clustername admin your_password

## 注意点

  * opencsv-3.9.jarをダウンロードしてください。
  * プログラミングチュートリアルのプログラムを一部修正しています。

