package pvrms;

import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;

import com.opencsv.CSVReader;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.RowKey;

// 設備情報
class Equip {
	@RowKey String id;
	String   name;
	//Blob     spec;	// 簡単化のため、仕様情報は未使用とする
}

public class SimplePv0 {

    /*
     * CSVファイルから、設備情報をロードする
     */
    public static void main(String[] args) throws GSException, ParseException, IOException {

        final String equipColName = "equipment_col";
        if (args.length != 5) {
                System.out.println("Usage:pvrms.SimplePv0 Addr Port ClusterName User Passwd ");
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
         * CSVファイルの読み込み
         *
         *先頭行はセンサID　　
         *    センサIDは、センサ名1,センサ名2,センサ名3･･･
         *それ以降の行はデータ　同一時刻に発生する各センサの測定値
         *    データは、date,time,センサ1値,ステータス,センサ2値,ステータス, ...
         */
        String dataFileName = "equipName.csv";
        CSVReader reader = new CSVReader(new FileReader(dataFileName));
        String[] nextLine;

        //  コレクションを作成
        Collection<String,Equip> equipCol = store.putCollection(equipColName, Equip.class);

        // カラムに索引を設定 　カラム　タイプがStringなのでTREE索引
        equipCol.createIndex("id");
        equipCol.createIndex("name");

        // 自動コミットモードをオフ
        equipCol.setAutoCommit(false);

        //コミット間隔
        Long commitInterval = (long) 1;

        //値を登録
        Equip equip = new Equip();
        Long cnt = (long) 0;
        byte[] b = new byte[1];
        b[0] = 1;

        while ((nextLine = reader.readNext()) != null) {
            // 設備情報登録
            equip.id		 = nextLine[0];
            equip.name		 = nextLine[1];
            equipCol.put(equip);
            cnt++;
            if(0 == cnt%commitInterval) {
                // トランザクションの確定
                equipCol.commit();
            }
        }
        // トランザクションの確定
        equipCol.commit();
        System.out.println("◆　equip_colコンテナを作成し、ROWを"+cnt+"件登録しました。◆");
        // リソースの解放
        store.close();
        reader.close();
    }
}