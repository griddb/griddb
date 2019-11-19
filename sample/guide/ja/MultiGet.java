import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.ContainerType;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowKeyPredicate;

public class MultiGet {

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
			// コレクションを作成&ロウ登録する
			//===============================================
			createContainerPutRow(store);


			//===============================================
			// 複数のコンテナから一括でロウを取得する
			//===============================================
			// (1)取得条件を構築する
			Map<String, RowKeyPredicate<Integer>> predMap = new HashMap<String, RowKeyPredicate<Integer>>();
			{
				RowKeyPredicate<Integer> predicate = RowKeyPredicate.create(Integer.class);
				predicate.add(0);
				predMap.put("SampleJava_MultiGet1", predicate);
			}
			{
				RowKeyPredicate<Integer> predicate = RowKeyPredicate.create(Integer.class);
				predicate.add(2);
				predicate.add(4);
				predMap.put("SampleJava_MultiGet2", predicate);
			}

			// (2)複数コンテナからロウを取得する
			Map<String, List<Row>> outMap = store.multiGet(predMap);

			System.out.println("MultiGet");

			// (3)ロウの値を取得する
			for (Map.Entry<String, List<Row>> entry : outMap.entrySet()) {
				System.out.println("containerName="+entry.getKey());

				for (Row row : entry.getValue()) {
					int id = row.getInteger(0);
					String name = row.getString(1);
					int count = row.getInteger(2);

					System.out.println("    id=" + id + " name=" + name +" count=" + count);
				}
			}

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

	private static void createContainerPutRow(GridStore store) throws Exception {
		// コンテナ情報作成
		ContainerInfo containerInfo = new ContainerInfo();
		containerInfo.setType(ContainerType.COLLECTION);
		List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
		columnList.add(new ColumnInfo("id", GSType.INTEGER));
		columnList.add(new ColumnInfo("productName", GSType.STRING));
		columnList.add(new ColumnInfo("count", GSType.INTEGER));
		containerInfo.setColumnInfoList(columnList);
		containerInfo.setRowKeyAssigned(true);

		{
			// コレクション作成
			Container<?, Row> container = store.putContainer("SampleJava_MultiGet1", containerInfo, false);

			// ロウ登録
			String[] nameList = {"notebook PC", "desktop PC", "keyboard", "mouse", "printer"};
			int[] numberList = {108, 72, 25, 45, 62};
			List<Row> rowList = new ArrayList<Row>();
			for ( int i = 0; i < nameList.length; i++ ){
				Row row = container.createRow();
				row.setInteger(0, i);
				row.setString(1, nameList[i]);
				row.setInteger(2, numberList[i]);
				rowList.add(row);
			}
			container.put(rowList);

			System.out.println("Create Collection name=SampleJava_MultiGet1");
		}
		{
			// コンテナ作成
			Container<?, Row> container = store.putContainer("SampleJava_MultiGet2", containerInfo, false);

			// ロウ登録
			String[] nameList = {"notebook PC", "desktop PC", "keyboard", "mouse", "printer"};
			int[] numberList = {50, 11, 208, 23, 153};
			List<Row> rowList = new ArrayList<Row>();
			for ( int i = 0; i < nameList.length; i++ ){
				Row row = container.createRow();
				row.setInteger(0, i);
				row.setString(1, nameList[i]);
				row.setInteger(2, numberList[i]);
				rowList.add(row);
			}
			container.put(rowList);

			System.out.println("Create Collection name=SampleJava_MultiGet2");
		}

	}
}