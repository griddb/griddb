import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.ContainerType;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.IndexInfo;
import com.toshiba.mwcloud.gs.IndexType;
import com.toshiba.mwcloud.gs.Row;

public class CreateIndex {

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
			String containerName = "SampleJava_Index";
			createContainer(store, containerName);


			//===============================================
			// 索引を作成する
			//===============================================
			// Containerオブジェクトの取得
			Container<?, Row> container = store.getContainer(containerName);
			if ( container == null ){
				throw new Exception("Container not found.");
			}

			// (1)索引情報を設定する
			IndexInfo indexInfo = new IndexInfo();
			indexInfo.setColumnName("count");
			indexInfo.setType(IndexType.TREE);

			// (2)索引を作成する
			container.createIndex(indexInfo);

			System.out.println("Create Index");

			//===============================================
			// 終了処理
			//===============================================
			container.close();
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