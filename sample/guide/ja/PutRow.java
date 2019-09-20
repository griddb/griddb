import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Row;

public class PutRow {

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
			ContainerInfo containerInfo = new ContainerInfo();
			List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
			columnList.add(new ColumnInfo("id", GSType.INTEGER));
			columnList.add(new ColumnInfo("productName", GSType.STRING));
			columnList.add(new ColumnInfo("count", GSType.INTEGER));
			containerInfo.setColumnInfoList(columnList);
			containerInfo.setRowKeyAssigned(true);

			String containerName = "SampleJava_PutRow";
			store.putCollection(containerName, containerInfo, false);
			System.out.println("Create Collection name="+containerName);


			//===============================================
			// ロウを登録する
			//===============================================
			//(1) Containerオブジェクトの取得
			Container<?, Row> container = store.getContainer(containerName);
			if ( container == null ){
				throw new Exception("Container not found.");
			}

			//(2) 空のロウオブジェクトの作成
			Row row = container.createRow();

			//(3) ロウオブジェクトにカラム値をセット
			row.setInteger(0, 0);
			row.setString(1, "display");
			row.setInteger(2, 150);

			//(4) ロウの登録
			container.put(row);

			System.out.println("Put Row num=1");

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
}