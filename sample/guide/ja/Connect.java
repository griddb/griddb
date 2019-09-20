import java.util.Properties;

import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;

public class Connect {

	public static void main(String[] args){
		try {
			//===============================================
			// クラスタに接続する
			//===============================================
			//(1)接続情報を指定する (マルチキャスト方式)
			Properties prop = new Properties();
			prop.setProperty("notificationAddress", "239.0.0.1");
			prop.setProperty("notificationPort", "31999");
			prop.setProperty("clusterName", "myCluster");
			prop.setProperty("database", "public");
			prop.setProperty("user", "admin");
			prop.setProperty("password", "admin");
			prop.setProperty("applicationName", "SampleJava");

			/*
			// (固定リスト方式の場合)
			Properties prop = new Properties();
			prop.setProperty("notificationMember", "192.168.1.10:10001,192.168.1.11:10001,192.168.1.12:10001");	// ノード3台の例
			prop.setProperty("clusterName", "myCluster");
			prop.setProperty("database", "public");
			prop.setProperty("user", "admin");
			prop.setProperty("password", "admin");
			prop.setProperty("applicationName", "SampleJava");
			*/
			/*
			// (プロバイダ方式の場合)
			Properties prop = new Properties();
			prop.setProperty("notificationProvider", "http://example.com/notification/provider");
			prop.setProperty("clusterName", "myCluster");
			prop.setProperty("database", "public");
			prop.setProperty("user", "admin");
			prop.setProperty("password", "admin");
			prop.setProperty("applicationName", "SampleJava");
			*/
			
			//(2)GridStoreオブジェクトを生成する
			GridStore store = GridStoreFactory.getInstance().getGridStore(prop);

			//(3)コンテナ作成や取得などの操作を行うと、クラスタに接続される
			store.getContainer("dummyContainer");

			System.out.println("Connect to cluster");

			//===============================================
			// 終了処理
			//===============================================
			// (4)接続をクローズする
			store.close();
			System.out.println("success!");

		} catch ( Exception e ){
			e.printStackTrace();
		}
	}
}