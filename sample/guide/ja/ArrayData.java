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

public class ArrayData {

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
			String containerName = "SampleJava_ArrayData";
			{
				// コンテナ情報オブジェクトを生成
				ContainerInfo containerInfo = new ContainerInfo();

				// カラムの名前やデータ型をカラム情報オブジェクトにセット
				List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
				columnList.add(new ColumnInfo("id", GSType.INTEGER));
				columnList.add(new ColumnInfo("string_array", GSType.STRING_ARRAY));
				columnList.add(new ColumnInfo("integer_array", GSType.INTEGER_ARRAY));
				containerInfo.setColumnInfoList(columnList);

				// ロウキーありの場合は設定する
				containerInfo.setRowKeyAssigned(true);

				// コンテナ作成
				store.putCollection(containerName, containerInfo, false);
				System.out.println("Create Collection name="+containerName);
			}


			//===============================================
			// 配列型のデータを登録する
			//===============================================
			Collection<Integer, Row> container = store.getCollection(containerName);
			if ( container == null ){
				throw new Exception("Container not found");
			}

			{
				// (1)配列型のデータ
				String[] stringArray = {"Sales", "Development", "Marketing", "Research"};
				int[] integerArray = {39, 92, 18, 51 };

				// (2)ロウに配列型のデータをセットする
				Row row = container.createRow();
				row.setInteger(0, 0);
				row.setStringArray(1, stringArray);
				row.setIntegerArray(2, integerArray);

				// (3)ロウを登録する
				container.put(row);

				System.out.println("Put Row (Array)");
			}

			//===============================================
			// 配列型のデータを取得する
			//===============================================
			{
				// (1)ロウを取得
				Row row = container.get(0);
				if ( row == null ){
					throw new Exception("Row not found");
				}

				// (2)ロウから配列型データを取得
				String[] stringArray = row.getStringArray(1);
				int[] integerArray = row.getIntegerArray(2);

				System.out.print("Get Row (Array) stringArray=");
				for ( String str : stringArray ){
					System.out.print(str + ", ");
				}
				System.out.print("integerArray=");
				for ( int tmp : integerArray ){
					System.out.print(tmp + ", ");
				}
				System.out.println();

				container.close();
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


}