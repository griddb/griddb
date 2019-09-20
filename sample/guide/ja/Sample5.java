package test;


import java.util.ArrayList;
import java.util.Arrays;
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
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowKeyPredicate;
import com.toshiba.mwcloud.gs.RowSet;


// 複数コンテナ一括操作
public class Sample5 {

	public static void main(String[] args) throws GSException {
		final List<String> containerNameList = Arrays.asList(
				"col01", "col02", "col03", "col04", "col05");
		final List<String> keyList = Arrays.asList(
				"name01", "name02");

		// GridStoreインスタンスの取得
		Properties props = new Properties();
		props.setProperty("notificationAddress", args[0]);
		props.setProperty("notificationPort", args[1]);
		props.setProperty("clusterName", args[2]);
		props.setProperty("user", args[3]);
		props.setProperty("password", args[4]);
		GridStore store = GridStoreFactory.getInstance().getGridStore(props);

		// コンテナ情報を作成
		ContainerInfo info = new ContainerInfo();
		info.setType(ContainerType.COLLECTION);
		info.setColumnInfoList(Arrays.asList(
				new ColumnInfo("name", GSType.STRING),
				new ColumnInfo("status", GSType.BOOL),
				new ColumnInfo("count", GSType.LONG),
				new ColumnInfo("lob", GSType.BYTE_ARRAY)));
		info.setRowKeyAssigned(true);

		// コレクションの作成
		List<Container<String, Row>> containerList =
				new ArrayList<Container<String, Row>>();
		for (String containerName : containerNameList) {
			Container<String, Row> container =
					store.putContainer(containerName, info, false);
			containerList.add(container);
		}

		// 複数コレクションのRowを一括登録
		{
			boolean status = false;
			long count = 1;
			byte[] lob = new byte[] { 65, 66, 67, 68, 69, 70, 71, 72, 73, 74 };

			Map<String, List<Row>> inMap = new HashMap<String, List<Row>>();
			for (String containerName : containerNameList) {
				List<Row> rowList = new ArrayList<Row>();
				for (String key : keyList) {
					Row row = store.createRow(info);
					row.setString(0, key);
					row.setBool(1, status);
					row.setLong(2, count);
					row.setByteArray(3, lob);
					count++;

					rowList.add(row);
				}
				inMap.put(containerName, rowList);
			}
			store.multiPut(inMap);
		}

		// 複数コレクションのRowを一括取得
		{
			// 取得条件を構築
			Map<String, RowKeyPredicate<String>> predMap =
					new HashMap<String, RowKeyPredicate<String>>();
			for (String containerName : containerNameList) {
				RowKeyPredicate<String> predicate =
						RowKeyPredicate.create(String.class);
				for (String key : keyList) {
					predicate.add(key);
				}
				predMap.put(containerName, predicate);
			}

			Map<String, List<Row>> outMap = store.multiGet(predMap);

			// 取得結果を出力
			for (Map.Entry<String, List<Row>> entry : outMap.entrySet()) {
				for (Row row : entry.getValue()) {
					String name = row.getString(0);
					long count = row.getLong(2);

					System.out.println("Person[" + entry.getKey() + "]:" +
							" name=" + name +
							" count=" + count);
				}
			}
		}

		// 複数コレクションのRowを一括検索(クエリ使用)
		{
			List<Query<Row>> queryList = new ArrayList<Query<Row>>();
			for (Container<String, Row> container : containerList) {
				String tql = "select * where count >= 0";
				queryList.add(container.query(tql));
			}

			store.fetchAll(queryList);

			for (int i = 0; i < queryList.size(); i++) {
				Query<Row> query = queryList.get(i);
				RowSet<Row> rs = query.getRowSet();
				while (rs.hasNext()) {
					Row row = rs.next();

					String name = row.getString(0);
					boolean status = row.getBool(1);
					long count = row.getLong(2);
					byte[] lob = row.getByteArray(3);

					System.out.println("Person[" + i + "]:" +
							" name=" + name +
							" status=" + status +
							" count=" + count +
							" lob=" + Arrays.toString(lob));
				}
			}
		}

		// リソースの解放
		store.close();
	}

}