package pvrms;

import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import com.opencsv.CSVReader;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.RowKey;

// アラート情報
class Alert {
    @RowKey Long id;
    Date    timestamp;
    String  sensorId;
    int     level;
    String  detail;
}

public class SimplePv1 {

    /*
     * CSVファイルから、アラートデータをロードする
     */
    public static void main(String[] args) throws GSException, ParseException, IOException {

        final String alertColName = "alert_col";
        if (args.length != 5) {
                System.out.println("Usage:pvrms.SimplePv1 Addr Port ClusterName User Passwd ");
                System.exit(1);
        }

        // GridStore接続時のパラメータ設定
        Properties props = new Properties();
        props.setProperty("notificationAddress", args[0]);
        props.setProperty("notificationPort", args[1]);
        props.setProperty("clusterName", args[2]);
        props.setProperty("user", args[3]);
        props.setProperty("password", args[4]);
        GridStore	store = GridStoreFactory.getInstance().getGridStore(props);

        // CSVファイルの読み込み
        String dataFileName = "alarmHistory.csv";
        CSVReader reader = new CSVReader(new FileReader(dataFileName));
        String[] nextLine;

        // コレクションを作成
        Collection<Long,Alert> alertCol = store.putCollection(alertColName, Alert.class);

        // カラムに索引を設定 　カラム　タイプがDate,StringなのでTREE索引
        alertCol.createIndex("timestamp");
        alertCol.createIndex("level");

        // 自動コミットモードをオフ
        alertCol.setAutoCommit(false);

        // コミット間隔
        Long commitInterval = (long) 24;

        // 値を登録
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Alert alert = new Alert();
        Long cnt = (long) 0;
        while ((nextLine = reader.readNext()) != null) {

            String dateS     = nextLine[0];
            String timeS     = nextLine[1];
            String datetimeS = dateS + " " + timeS ;
            Date   date      = format.parse(datetimeS);

            alert.id         = ++cnt;
            alert.timestamp  = date;
            alert.sensorId   = nextLine[2];
            alert.level      = Integer.valueOf(nextLine[3]);
            alert.detail     = nextLine[4];

            alertCol.put(alert);

            if(0 == cnt%commitInterval) {
                // トランザクションの確定
                alertCol.commit();
            }
        }
        // トランザクションの確定
        alertCol.commit();
        System.out.println("◆　alert_colコンテナを作成し、ROWを"+cnt+"件登録しました。◆");

        // リソースの解放
        store.close();
        reader.close();
    }
}