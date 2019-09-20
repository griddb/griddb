import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.CompressionMethod;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.ContainerType;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.IndexType;
import com.toshiba.mwcloud.gs.TimeSeriesProperties;

public class ContainerInformation {

	public static void main(String[] args){
		try {
			//===============================================
			// クラスタに接続する
			//===============================================
			// 接続情報を指定
			Properties prop = new Properties();
			prop.setProperty("notificationAddress", "239.0.0.1");
			prop.setProperty("notificationPort", "31999");
			prop.setProperty("clusterName", "myCluster");
			prop.setProperty("database", "public");
			prop.setProperty("user", "admin");
			prop.setProperty("password", "admin");
			prop.setProperty("applicationName", "SampleJava");

			// GridStoreオブジェクトを生成
			GridStore store = GridStoreFactory.getInstance().getGridStore(prop);
			// コンテナ作成や取得などの操作を行うと、クラスタに接続される
			store.getContainer("dummyContainer");


			//===============================================
			// コレクションを作成する
			//===============================================
			String containerName = "SampleJava_Info";
			createContainer(store, containerName);


			//===============================================
			// コンテナ情報を取得する
			//===============================================
			// (1)コンテナ情報の取得
			ContainerInfo containerInfo = store.getContainerInfo(containerName);

			// (2)名前とタイプの取得
			String name = containerInfo.getName();
			ContainerType type = containerInfo.getType();
			System.out.println("name=" + name +", type="+type);

			// (3)カラム情報の取得
			for (int i = 0; i < containerInfo.getColumnCount(); i++){
				ColumnInfo info = containerInfo.getColumnInfo(i);
				// カラム名
				String columnName = info.getName();

				// カラムのデータ型
				GSType columnType = info.getType();

				// NotNull制約 (true=Not Null制約なし, false=Not Null制約あり)
				boolean columnNullable = info.getNullable();

				// 索引情報
				Set<IndexType> indexTypes = info.getIndexTypes();

				System.out.println("column name="+columnName+", type="+columnType+", nullable="+columnNullable);
				if ( indexTypes.size() > 0 ) System.out.println("       indexTypes="+indexTypes);
			}

			// (4)時系列コンテナ特有の情報の取得
			TimeSeriesProperties tmProperties = containerInfo.getTimeSeriesProperties();
			if ( tmProperties != null ){
				// 圧縮方式
				CompressionMethod method = tmProperties.getCompressionMethod();
				// ・・・TimeSeriesPropertiesから必要な情報を取得する
			}

			//===============================================
			// 終了処理
			//===============================================
			store.close();
			System.out.println("success!");

		} catch ( Exception e ){
			e.printStackTrace();
		}
	}
	private static void createContainer(GridStore store, String containerName) throws Exception {
		// コンテナ情報オブジェクトを生成
		ContainerInfo containerInfo = new ContainerInfo();
		containerInfo.setType(ContainerType.COLLECTION);

		// カラムの名前やデータ型をカラム情報オブジェクトにセット
		List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
		columnList.add(new ColumnInfo("id", GSType.INTEGER));
		columnList.add(new ColumnInfo("productName", GSType.STRING));
		columnList.add(new ColumnInfo("count", GSType.INTEGER));
		containerInfo.setColumnInfoList(columnList);

		// ロウキーありの場合は設定する
		containerInfo.setRowKeyAssigned(true);

		// コンテナ作成
		store.putContainer(containerName, containerInfo, false);
		System.out.println("Create Collection name="+containerName);

	}
}