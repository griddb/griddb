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


// Operation on Collection data
public class Sample1 {

	static class Person {
		@RowKey String name;
		boolean status;
		long count;
		byte[] lob;
	}

	public static void main(String[] args) throws GSException {

		// Acquiring a GridStore instance
		Properties props = new Properties();
		props.setProperty("notificationAddress", args[0]);
		props.setProperty("notificationPort", args[1]);
		props.setProperty("clusterName", args[2]);
		props.setProperty("user", args[3]);
		props.setProperty("password", args[4]);
		GridStore store = GridStoreFactory.getInstance().getGridStore(props);

		// Creating a collection
		Collection<String, Person> col = store.putCollection("col01", Person.class);

		// Setting an index for a column
		col.createIndex("count");

		// Setting auto-commit off
		col.setAutoCommit(false);

		// Preparing row data
		Person person = new Person();
		person.name = "name01";
		person.status = false;
		person.count = 1;
		person.lob = new byte[] { 65, 66, 67, 68, 69, 70, 71, 72, 73, 74 };

		// Operating a row in KV format: RowKey is "name01"
		boolean update = true;
		col.put(person);	// Registration
		person = col.get(person.name, update);	// Aquisition (Locking to update)
		col.remove(person.name);	// Deletion

		// Operating a row in KV format: RowKey is "name02"
		col.put("name02", person);	// Registration (Specifying RowKey)

		// Committing transaction (releasing lock)
		col.commit();

		// Search rows in a container
		Query<Person> query = col.query("select * where name = 'name02'");

		// Fetching and updating retrieved rows
		RowSet<Person> rs = query.fetch(update);
		while (rs.hasNext()) {
			// Update the searched Row
			Person person1 = rs.next();
			person1.count += 1;
			rs.update(person1);

			System.out.println("Person:" +
					" name=" + person1.name +
					" status=" + person1.status +
					" count=" + person1.count +
					" lob=" + Arrays.toString(person1.lob));
		}

		// Committing transaction
		col.commit();

		// Releasing resource
		store.close();
	}

}