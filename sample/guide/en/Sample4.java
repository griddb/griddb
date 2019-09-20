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


// Schema definition using container information
public class Sample4 {

	public static void main(String[] args) throws GSException {

		// Acquiring a GridStore instance
		Properties props = new Properties();
		props.setProperty("notificationAddress", args[0]);
		props.setProperty("notificationPort", args[1]);
		props.setProperty("clusterName", args[2]);
		props.setProperty("user", args[3]);
		props.setProperty("password", args[4]);
		GridStore store = GridStoreFactory.getInstance().getGridStore(props);

		// Creating a container information
		ContainerInfo info = new ContainerInfo();
		info.setType(ContainerType.COLLECTION);
		info.setName("col01");
		info.setColumnInfoList(Arrays.asList(
				new ColumnInfo("name", GSType.STRING),
				new ColumnInfo("status", GSType.BOOL),
				new ColumnInfo("count", GSType.LONG),
				new ColumnInfo("lob", GSType.BYTE_ARRAY)));
		info.setRowKeyAssigned(true);

		// Creating a collection
		Container<String, Row> container = store.putContainer(null, info, false);

		// Setting an index for a column
		container.createIndex("count");

		// Setting auto-commit off
		container.setAutoCommit(false);

		// Preparing row data
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

		// Operating a row in KV format: RowKey is "name01"
		boolean update = true;
		container.put(row);	// Registration
		row = container.get("name01", update);	// Acquisition (Locking to update)
		container.remove("name01");	// Deletion

		// Operating a row in KV format: RowKey is "name02"
		container.put("name02", row);	// Registration (Specifying RowKey)

		// Committing transaction (Releasing lock)
		container.commit();

		// Search rows in a container
		Query<Row> query = container.query("select * where name = 'name02'");

		// Fetching and updating retrieved rows
		RowSet<Row> rs = query.fetch(update);
		while (rs.hasNext()) {
			// Updating retrived rows
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

		// Committing transaction
		container.commit();

		// Releasing resource
		store.close();
	}

}