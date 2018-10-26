package pvrms;

import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import com.opencsv.CSVReader;

import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.RowKey;
import com.toshiba.mwcloud.gs.TimeSeries;

// センサデータ
class Point {
    @RowKey Date time;
    double  value;
    String  status;
}

public class SimplePv2 {

    /*
     * CSVファイルから、時系列データをロードする
     */
    public static void main(String[] args) throws ParseException, IOException {

        if (args.length != 5) {
                System.out.println("Usage:pvrms.SimplePv2 Addr Port ClusterName User Passwd ");
                System.exit(1);
        }

        // GridStore接続時のパラメータ設定
        Properties props = new Properties();
        props.setProperty("notificationAddress", args[0]);
        props.setProperty("notificationPort", args[1]);
        props.setProperty("clusterName", args[2]);
        props.setProperty("user", args[3]);
        props.setProperty("password", args[4]);

        GridStore store = GridStoreFactory.getInstance().getGridStore(props);

        /*
         *  CSVファイルの読み込み
         *
         *先頭行はセンサID　　
         *    センサIDは、 センサ名1,センサ名2,センサ名3･･･
         *それ以降の行はデータ　同一時刻に発生する各センサの測定値
         *    データは、 date,time,センサ1値,ステータス,センサ2値,ステータス, ...
         */

        String dataFileName = "sensorHistory.csv";
        CSVReader reader = new CSVReader(new FileReader(dataFileName));
        String[] nextLine;
        nextLine = reader.readNext();

        //センサIDを読み込み、時系列を作成
        String[] tsNameArray = new String[nextLine.length];

        for(int j = 0; j < nextLine.length; j++) {
            tsNameArray[j] = nextLine[j];
            store.putTimeSeries(tsNameArray[j], Point.class);
        }

        //各時系列に値を登録
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Point point             = new Point();
        Long cnt = (long) 0;
        while ((nextLine = reader.readNext()) != null) {
            String dateS     = nextLine[0];
            String timeS     = nextLine[1];
            String datetimeS = dateS + " " + timeS ;
            Date   date      = format.parse(datetimeS);
            for(int i = 0, j = 2; j < nextLine.length; i++, j+=2) {
                TimeSeries<Point> ts = store.getTimeSeries(tsNameArray[i], Point.class);
                point.time   = date;
                point.value  = Double.valueOf(nextLine[j]);
                point.status = nextLine[j+1];
                //指定した時刻でデータを登録　現在時刻の場合はts.append(point)　
                //autocommitなので1件ずつトランザクションは確定
                ts.put(date,point);
            }
            cnt++;
        }
        System.out.println("◆　時系列コンテナ"+tsNameArray.length+"件を作成し、ROWを"+cnt+"件ずつ登録しました。◆");
        // リソースの解放
        store.close();
        reader.close();
    }
}