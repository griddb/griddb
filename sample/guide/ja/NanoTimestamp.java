import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import java.sql.Timestamp;//

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.TimeUnit;//
import com.toshiba.mwcloud.gs.TimestampUtils;

public class NanoTimestamp {

	public static void main(String[] args){
		try {
			//===============================================
			// [Connect]
			// クラスタに接続する
			//===============================================
			// 接続情報を指定する (マルチキャスト方式/Multicast method)
			Properties prop = new Properties();
			prop.setProperty("notificationAddress", "239.0.0.1");
			prop.setProperty("notificationPort", "31999");
			prop.setProperty("clusterName", "myCluster");
			prop.setProperty("database", "public");
			prop.setProperty("user", "admin");
			prop.setProperty("password", "admin");
			prop.setProperty("applicationName", "SampleJava");

			/*
			// 接続情報を指定する (固定リスト方式/Fixed list method)
			Properties prop = new Properties();
			prop.setProperty("notificationMember", "127.0.0.1:10001");
			prop.setProperty("clusterName", "myCluster");
			prop.setProperty("database", "public");
			prop.setProperty("user", "admin");
			prop.setProperty("password", "admin");
			prop.setProperty("applicationName", "SampleJava");
			*/

			// GridStoreオブジェクトを生成する
			GridStore store = GridStoreFactory.getInstance().getGridStore(prop);
			// コンテナ作成や取得などの操作を行うと、クラスタに接続される
			store.getContainer("dummyContainer");

			//===============================================
			// [Create Container]
			// コレクションを作成する
			//===============================================
			// (1)コンテナ情報オブジェクトを生成
			ContainerInfo containerInfo = new ContainerInfo();

			// (2)カラムの名前やデータ型をカラム情報オブジェクトにセット
			List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
			columnList.add(new ColumnInfo.Builder(
				new ColumnInfo("time", GSType.TIMESTAMP))
				.setTimePrecision(TimeUnit.NANOSECOND).toInfo());//
			columnList.add(new ColumnInfo("productName", GSType.STRING));
			columnList.add(new ColumnInfo("value", GSType.INTEGER));
			columnList.add(new ColumnInfo.Builder(
				new ColumnInfo("time2", GSType.TIMESTAMP))
				.setTimePrecision(TimeUnit.MICROSECOND).toInfo());//
			
			// (3)カラム情報をコンテナ情報オブジェクトに設定
			containerInfo.setColumnInfoList(columnList);

			// (4)ロウキーありの場合は設定する
			containerInfo.setRowKeyAssigned(true);

			// (5)コレクション作成
			Collection<Void, Row> collection = store.putCollection("SampleJava_collection1", containerInfo, false);

			System.out.println("Create Collection name=SampleJava_collection1");

			collection.close();

			//===============================================
			// [Put Row]
			// ロウを登録する
			//===============================================
			//(1) Containerオブジェクトの取得
			Container<?, Row> container = store.getContainer("SampleJava_collection1");
			if ( container == null ){
				throw new Exception("Container not found.");
			}

			//(2) 空のロウオブジェクトの作成
			Row row = container.createRow();

			Timestamp ts = TimestampUtils.parsePrecise("2023-05-18T10:41:26.123456789Z");

			//(3) ロウオブジェクトにカラム値をセット
			row.setPreciseTimestamp(0, ts);
			row.setString(1, "display");
			row.setInteger(2, 1);
			row.setPreciseTimestamp(3, ts);
			
			//(4) ロウの登録
			container.put(row);

			System.out.println("Put Row num=1");

			container.close();

			//===============================================
			// [Get Row]
			// ロウを取得する
			//===============================================
			// (1)Containerオブジェクトの取得
			Container<Timestamp, Row> container2 = store.getContainer("SampleJava_collection1");
			if ( container2 == null ){
				throw new Exception("Container not found.");
			}

			// (2)ロウキーを指定してロウを取得する
			row = container2.get(ts);
			if ( row == null ){
				throw new Exception("Row not found");
			}

			// (3)ロウからカラムの値を取り出す
			Timestamp ts1 = row.getPreciseTimestamp(0);
			String name = row.getString(1);
			int value = row.getInteger(2);
			Timestamp ts2 = row.getPreciseTimestamp(3);

			System.out.print("Get Row time=" + TimestampUtils.formatPrecise(ts1, null));
			System.out.print(", productName=" + name + ", value=" + value);
			System.out.println(", time2=" + TimestampUtils.formatPrecise(ts2, null));

			container2.close();
			
			//===============================================
			// 終了処理
			//===============================================

			store.close();
			System.out.println("success!");

		} catch ( Exception e ){
			e.printStackTrace();
		}
	}
}