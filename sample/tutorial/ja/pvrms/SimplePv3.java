package pvrms;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.TimeUnit;
import com.toshiba.mwcloud.gs.TimestampUtils;

public class SimplePv3 {

    /*
     *  24時間以内に発生した重大アラートについて、設備情報と直前の時系列データを表示する
     *  本サンプルでは、alert_colに登録した時刻に合わせて検索します。
     */
    public static void main(String[] args) throws GSException, ParseException, IOException {

        final String alertColName = "alert_col";
        final String equipColName = "equipment_col";

        if (args.length != 5) {
            System.out.println("Usage:pvrms.SimplePv3 Addr Port ClusterName User Passwd ");
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
        Collection<Long,Alert> alertCol = store.getCollection(alertColName, Alert.class);

        //アラートの監視時間を設定　現在時刻の場合　new Date(System.currentTimeMillis())
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS",Locale.JAPAN);
        Date tm1=sf.parse("2015-01-02 08:00:00.000");

        System.out.println("◆　alert_colから指定した日時の24時間前以降の重大アラート（>レベル3)を検索◆");

        //String from =    TimestampUtils.format( TimestampUtils.add(new Date(System.currentTimeMillis()), -24, TimeUnit.HOUR) );
        String from =    TimestampUtils.format( TimestampUtils.add(tm1, -24, TimeUnit.HOUR) );
        Query<Alert> alertQuery = alertCol.query( "select * where level > 3 and " +
                                                     "timestamp > " + "TIMESTAMP('" + from + "')" );

        // 検索式を出力 検索時の時刻はUTC
        System.out.println("select * where level > 3 and " +
                              "timestamp > " + "TIMESTAMP('" + from + "')");
        RowSet<Alert> alertRs = alertQuery.fetch();

        System.out.println("◆　重大アラートが発生したセンサの設備情報とアラート直前10分のデータを表示◆");
        System.out.println("◆　       アラート数　:　"+alertRs.size()+"　◆");
        while( alertRs.hasNext() ) {
            Alert seriousAlert = alertRs.next();

            // センサIDとセンサ種別を取得
            String sensorId = seriousAlert.sensorId;
            String sensorType = sensorId.substring(sensorId.indexOf("_")+1);

            // 設備を検索
            Collection<String,Equip> equipCol = store.getCollection(equipColName, Equip.class);
            Equip equip = equipCol.get(sensorId);

            System.out.println("[Equipment] " + equip.name  + " (sensor) "+ sensorType);
            System.out.println("[Detail] " + seriousAlert.detail);

            //直前の時系列データを検索
            String tsName = seriousAlert.sensorId;
            TimeSeries<Point> ts = store.getTimeSeries(tsName, Point.class);
            Date endDate   = seriousAlert.timestamp;
            Date startDate = TimestampUtils.add(seriousAlert.timestamp, -10, TimeUnit.MINUTE);
            RowSet<Point> rowSet =  ts.query(startDate, endDate).fetch();
            while (rowSet.hasNext()) {
                Point ret = rowSet.next();
                System.out.println(
                        "[Result] " +sf.format(ret.time) +
                        " " + ret.value + " " + ret.status);
            }
            System.out.println("");
        }

        // リソースの解放
        store.close();
    }
}