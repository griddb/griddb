package test;


import java.util.Arrays;
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


// コンテナ情報を用いてスキーマ定義
public class Sample4 {

	public static void main(String[] args) throws GSException {

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
		info.setName("col01");
		info.setColumnInfoList(Arrays.asList(
				new ColumnInfo("name", GSType.STRING),
				new ColumnInfo("status", GSType.BOOL),
				new ColumnInfo("count", GSType.LONG),
				new ColumnInfo("lob", GSType.BYTE_ARRAY)));
		info.setRowKeyAssigned(true);

		// コレクションの作成
		Container<String, Row> container = store.putContainer(null, info, false);

		// カラムに索引を設定
		container.createIndex("count");

		// 自動コミットモードをオフ
		container.setAutoCommit(false);

		// Rowのデータを用意
		Row row;
		{
			String name = "name01";
			boolean status = false;
			long count = 1;
			byte[] lob = new byte[] { 65, 66, 67, 68, 69, 70, 71, 72, 73, 74 };

			row = container.createRow();
			row.setString(0, name);
			row.setBool(1, status);
			row.setLong(2, count);
			row.setByteArray(3, lob);
		}

		// KV形式でRowを操作: RowKeyは"name01"
		boolean update = true;
		container.put(row);	// 登録
		row = container.get("name01", update);	// 取得(更新用にロック)
		container.remove("name01");	// 削除

		// KV形式でRowを操作: RowKeyは"name02"
		container.put("name02", row);	// 登録(RowKeyを指定)

		// トランザクションの確定(ロック解除)
		container.commit();

		// コレクション内のRowを検索
		Query<Row> query = container.query("select * where name = 'name02'");

		// 検索したRowの取得と更新
		RowSet<Row> rs = query.fetch(update);
		while (rs.hasNext()) {
			// 検索したRowの更新
			row = rs.next();

			String name = row.getString(0);
			boolean status = row.getBool(1);
			long count = row.getLong(2);
			byte[] lob = row.getByteArray(3);
			count += 1;
			rs.update(row);

			System.out.println("Person:" +
					" name=" + name +
					" status=" + status +
					" count=" + count +
					" lob=" + Arrays.toString(lob));
		}

		// トランザクションの確定
		container.commit();

		// リソースの解放
		store.close();
	}

}