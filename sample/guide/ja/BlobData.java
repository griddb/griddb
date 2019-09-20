import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Row;

public class BlobData {

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
			String containerName = "SampleJava_BlobData";
			{
				// コンテナ情報オブジェクトを生成
				ContainerInfo containerInfo = new ContainerInfo();

				// カラムの名前やデータ型をカラム情報オブジェクトにセット
				List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
				columnList.add(new ColumnInfo("id", GSType.INTEGER));
				columnList.add(new ColumnInfo("blob", GSType.BLOB));
				containerInfo.setColumnInfoList(columnList);

				// ロウキーありの場合は設定する
				containerInfo.setRowKeyAssigned(true);

				// コンテナ作成
				store.putCollection(containerName, containerInfo, false);
				System.out.println("Create Collection name="+containerName);
			}


			//===============================================
			// バイナリを登録する
			//===============================================
			Collection<Integer, Row> container = store.getCollection(containerName);
			{
				// (1)バイナリデータをファイルから読み込み、Blobを作成する
				FileInputStream blobFile = new FileInputStream(new File("BlobData.java"));
				Blob blob = container.createBlob();
				OutputStream blobBuffer = blob.setBinaryStream(1);
				int len = -1;
				while ((len = blobFile.read()) > -1) {
					blobBuffer.write(len);
				}
				blobBuffer.flush();

				// (2)ロウにバイナリをセットする
				Row row = container.createRow();
				row.setInteger(0, 0);
				row.setBlob(1, blob);

				// (3)ロウを登録する
				container.put(row);

				System.out.println("Put Row (Binary)");

				blobFile.close();
			}

			//===============================================
			// バイナリを取得する
			//===============================================
			{
				// (1)ロウを取得
				Row row = container.get(0);

				// (2)ロウからバイナリを取得
				Blob blob = row.getBlob(1);

				// (3)バイナリをファイルに出力する
				byte[] buffer = blob.getBytes(1, (int)blob.length());
				File file = new File("output");
				DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
				dos.write(buffer);
				dos.close();

				System.out.println("Get Row (Binary)");
			}

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