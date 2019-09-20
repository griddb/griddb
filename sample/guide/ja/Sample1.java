package test;


import java.util.Arrays;
import java.util.Properties;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.RowKey;
import com.toshiba.mwcloud.gs.RowSet;


// コレクションデータの操作
public class Sample1 {

	static class Person {
		@RowKey String name;
		boolean status;
		long count;
		byte[] lob;
	}

	public static void main(String[] args) throws GSException {

		// GridStoreインスタンスの取得
		Properties props = new Properties();
		props.setProperty("notificationAddress", args[0]);
		props.setProperty("notificationPort", args[1]);
		props.setProperty("clusterName", args[2]);
		props.setProperty("user", args[3]);
		props.setProperty("password", args[4]);
		GridStore store = GridStoreFactory.getInstance().getGridStore(props);

		// コレクションの作成
		Collection<String, Person> col = store.putCollection("col01", Person.class);

		// カラムに索引を設定
		col.createIndex("count");

		// 自動コミットモードをオフ
		col.setAutoCommit(false);

		// Rowのデータを用意
		Person person = new Person();
		person.name = "name01";
		person.status = false;
		person.count = 1;
		person.lob = new byte[] { 65, 66, 67, 68, 69, 70, 71, 72, 73, 74 };

		// KV形式でRowを操作: RowKeyは"name01"
		boolean update = true;
		col.put(person);	// 登録
		person = col.get(person.name, update);	// 取得(更新用にロック)
		col.remove(person.name);	// 削除

		// KV形式でRowを操作: RowKeyは"name02"
		col.put("name02", person);	// 登録(RowKeyを指定)

		// トランザクションの確定(ロック解除)
		col.commit();

		// コレクション内のRowを検索
		Query<Person> query = col.query("select * where name = 'name02'");

		// 検索したRowの取得と更新
		RowSet<Person> rs = query.fetch(update);
		while (rs.hasNext()) {
			// 検索したRowの更新
			Person person1 = rs.next();
			person1.count += 1;
			rs.update(person1);

			System.out.println("Person:" +
					" name=" + person1.name +
					" status=" + person1.status +
					" count=" + person1.count +
					" lob=" + Arrays.toString(person1.lob));
		}

		// トランザクションの確定
		col.commit();

		// リソースの解放
		store.close();
	}

}