import java.util.ArrayList;
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
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowSet;

public class FetchAll {

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
			// 複数のコンテナにTQLを実行する
			//===============================================
			List<Query<Row>> queryList = new ArrayList<Query<Row>>();

			// (1)コレクション"SampleJava_FetchAll1"に対するTQLのクエリを生成する
			{
				Container<Integer, Row> container = store.getContainer("SampleJava_FetchAll1");
				if ( container == null ){
					throw new Exception("Container not found.");
				}
				queryList.add(container.query("select * where count > 60"));
			}
			// (2)コレクション"SampleJava_FetchAll2"に対するTQLのクエリを生成する
			{
				Container<Integer, Row> container = store.getContainer("SampleJava_FetchAll2");
				if ( container == null ){
					throw new Exception("Container not found.");
				}
				queryList.add(container.query("select * where count > 100"));
			}

			// (3)複数コンテナを一括検索する
			store.fetchAll(queryList);

			// (4)結果を取得する
			for (int i = 0; i < queryList.size(); i++) {
				System.out.println("SampleJava_FetchAll"+(i+1));
				Query<Row> query = queryList.get(i);
				RowSet<Row> rs = query.getRowSet();

				while (rs.hasNext()) {
					Row row = rs.next();
					int id = row.getInteger(0);
					String name = row.getString(1);
					int count = row.getInteger(2);
					System.out.println("    row id=" + id + ", name=" + name + ", count=" + count);
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
			// コンテナ作成
			Container<?, Row> container = store.putContainer("SampleJava_FetchAll1", containerInfo, false);

			// ロウ登録
			String[] nameList = {"notebook PC", "desktop PC", "keybord", "mouse", "printer"};
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

			System.out.println("Create Collection name=SampleJava_FetchAll1");
		}
		{
			// コンテナ作成
			Container<?, Row> container = store.putContainer("SampleJava_FetchAll2", containerInfo, false);

			// ロウ登録
			String[] nameList = {"notebook PC", "desktop PC", "keybord", "mouse", "printer"};
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

			System.out.println("Create Collection name=SampleJava_FetchAll2");
		}
	}
}