import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Row;

public class MultiPut {

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
			// コレクションを作成する
			//===============================================
			createContainer(store);


			//===============================================
			// 複数のコンテナにロウを登録する
			//===============================================
			Map<String, List<Row>> paramMap = new HashMap<String, List<Row>>();

			// (1)コレクション"SampleJava_MultiPut1"に対して登録するロウを生成する
			{
				String containerName = "SampleJava_MultiPut1";
				Container<Integer, Row> container = store.getContainer(containerName);
				if ( container == null ){
					throw new Exception("Container not found.");
				}

				// 登録するデータ
				String[] nameList = {"notebook PC", "desktop PC", "keyboard", "mouse", "printer"};
				int[] numberList = {55, 81, 39, 72, 14};

				// ロウにデータをセットする
				List<Row> rowList = new ArrayList<Row>();
				for ( int i = 0; i < nameList.length; i++ ){
					Row row = container.createRow();
					row.setInteger(0, (i+1));
					row.setString(1, nameList[i]);
					row.setInteger(2, numberList[i]);
					rowList.add(row);
				}
				paramMap.put(containerName, rowList);
			}

			// (2)時系列コンテナ"SampleJava_MultiPut2"に対して登録するロウを生成する
			{
				String containerName = "SampleJava_MultiPut2";
				Container<Integer, Row> container = store.getContainer(containerName);
				if ( container == null ){
					throw new Exception("Container not found.");
				}

				// 登録するデータ
				String[] dateList = {"2018/12/01 10:20:19.111+0900", "2018/12/02 03:25:45.023+0900",
						"2018/12/03 08:29:21.932+0900", "2018/12/04 21:55:48.153+0900"};
				double[] valueList = { 129.9, 13.2, 832.7, 52.9 };

				// 日付の変換フォーマット
				SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss.SSSZ");

				// ロウにデータをセットする
				List<Row> rowList = new ArrayList<Row>();
				for ( int i = 0; i < dateList.length; i++ ){
					Row row = container.createRow();
					row.setTimestamp(0, format.parse(dateList[i]));
					row.setDouble(1, valueList[i]);
					rowList.add(row);
				}
				paramMap.put(containerName, rowList);
			}

			// (3)複数のコンテナに対してロウを登録する
			store.multiPut(paramMap);

			System.out.println("MultiPut");

			//===============================================
			// 終了処理
			//===============================================
			store.close();
			System.out.println("success!");

		} catch ( GSException e ){
			Map<String, String> param = e.getParameters();
			for(Map.Entry<String, String> entry : param.entrySet()){
				System.out.println(entry.getKey() + ":" + entry.getValue());
			}

			e.printStackTrace();

		} catch ( Exception e ){
			e.printStackTrace();
		}
	}

	private static void createContainer(GridStore store) throws Exception {
		{
			// コンテナ情報作成
			ContainerInfo containerInfo = new ContainerInfo();
			List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
			columnList.add(new ColumnInfo("id", GSType.INTEGER));
			columnList.add(new ColumnInfo("productName", GSType.STRING));
			columnList.add(new ColumnInfo("count", GSType.INTEGER));
			containerInfo.setColumnInfoList(columnList);
			containerInfo.setRowKeyAssigned(true);

			// コレクション作成
			store.putCollection("SampleJava_MultiPut1", containerInfo, false);

			System.out.println("Create Collection name=SampleJava_MultiPut1");
		}
		{
			// コンテナ情報作成
			ContainerInfo containerInfo = new ContainerInfo();
			List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
			columnList.add(new ColumnInfo("date", GSType.TIMESTAMP));
			columnList.add(new ColumnInfo("value", GSType.DOUBLE));
			containerInfo.setColumnInfoList(columnList);
			containerInfo.setRowKeyAssigned(true);

			// 時系列コンテナ作成
			store.putTimeSeries("SampleJava_MultiPut2", containerInfo, false);

			System.out.println("Create TimeSeries name=SampleJava_MultiPut2");
		}
	}

}