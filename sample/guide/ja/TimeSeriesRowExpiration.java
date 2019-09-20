import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.TimeSeriesProperties;
import com.toshiba.mwcloud.gs.TimeUnit;

public class TimeSeriesRowExpiration {

	public static void main(String[] args){
		try {
			//===============================================
			// クラスタに接続する
			//===============================================
			// 接続情報を指定する (マルチキャスト方式)
			Properties prop = new Properties();
			prop.setProperty("notificationAddress", "239.0.0.1");
			prop.setProperty("notificationPort", "31999");
			prop.setProperty("clusterName", "myCluster");
			prop.setProperty("database", "public");
			prop.setProperty("user", "admin");
			prop.setProperty("password", "admin");
			prop.setProperty("applicationName", "SampleJava");

			// GridStoreオブジェクトを生成する
			GridStore store = GridStoreFactory.getInstance().getGridStore(prop);
			// コンテナ作成や取得などの操作を行うと、クラスタに接続される
			store.getContainer("dummyContainer");

			//===============================================
			// 時系列コンテナ作成する
			//===============================================
			// (1)コンテナ情報オブジェクトを生成
			ContainerInfo containerInfo = new ContainerInfo();

			// (2)カラムの名前やデータ型をカラム情報オブジェクトにセット
			List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
			columnList.add(new ColumnInfo("date", GSType.TIMESTAMP));
			columnList.add(new ColumnInfo("value", GSType.DOUBLE));
			containerInfo.setColumnInfoList(columnList);

			// (3)ロウキーを設定
			containerInfo.setRowKeyAssigned(true);

			// (4)時系列コンテナの情報を生成して、ロウ期限開放を設定する
			TimeSeriesProperties tsProp = new TimeSeriesProperties();
			tsProp.setRowExpiration(100, TimeUnit.DAY);
			tsProp.setExpirationDivisionCount(5);
			containerInfo.setTimeSeriesProperties(tsProp);

			// (5)時系列コンテナ作成
			String containerName = "SampleJava_RowExpiration";
			TimeSeries<Row> timeseries = store.putTimeSeries(containerName, containerInfo, false);

			System.out.println("Create TimeSeries name="+containerName);

			//===============================================
			// 終了処理
			//===============================================
			timeseries.close();
			store.close();
			System.out.println("success!");

		} catch ( GSException e ){
			e.printStackTrace();
		}
	}
}
