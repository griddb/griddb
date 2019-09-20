import java.util.List;
import java.util.Properties;

import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.PartitionController;

public class ContainerNames {

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
			// コンテナ名一覧を取得する
			//===============================================
			{
				// (1)パーティションコントローラと、パーティション数を取得する
				PartitionController partitionController = store.getPartitionController();
				int partitionCount = partitionController.getPartitionCount();

				// (2)パーティション数でループして、コンテナ名一覧を取得する
				for (int i = 0; i < partitionCount; i++) {
					List<String> containerNames = partitionController.getContainerNames(i, 0, null);
					if ( containerNames.size() > 0 ){
						System.out.println(containerNames);
					}
				}
			}
			store.close();

			System.out.println("success!");

		} catch ( Exception e ){
			e.printStackTrace();
		}
	}
}