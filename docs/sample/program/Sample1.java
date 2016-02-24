package gsSample;
import java.util.Arrays;
import java.util.Properties;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.RowKey;
import com.toshiba.mwcloud.gs.RowSet;


// Operaton on Collection data
public class Sample1 {

	static class Person {
		@RowKey String name;
		boolean status;
		long count;
		byte[] lob;
	}

	public static void main(String[] args) throws GSException {

		// Get a GridStore instance
		Properties props = new Properties();
		props.setProperty("notificationAddress", args[0]);
		props.setProperty("notificationPort", args[1]);
		props.setProperty("clusterName", args[2]);
		props.setProperty("user", args[3]);
		props.setProperty("password", args[4]);
		GridStore store = GridStoreFactory.getInstance().getGridStore(props);

		// Create a Collection (Delete if schema setting is NULL)
		Collection<String, Person> col = store.putCollection("col01", Person.class);

		// Set an index on the Row-key Column
		col.createIndex("name");

		// Set an index on the Column
		col.createIndex("count");

		// Set the autocommit mode to OFF
		col.setAutoCommit(false);

		// Prepare data for a Row
		Person person = new Person();
		person.name = "name01";
		person.status = false;
		person.count = 1;
		person.lob = new byte[] { 65, 66, 67, 68, 69, 70, 71, 72, 73, 74 };

		// Operate a Row on a K-V basis: RowKey = "name01"
		boolean update = true;
		col.put(person);	// Add a Row
		person = col.get(person.name, update);	// Obtain the Row (acquiring a lock for update)
		col.remove(person.name);	// Delete the Row

		// Operate a Row on a K-V basis: RowKey = "name02"
		col.put("name02", person);	// Add a Row (specifying RowKey)

		// Commit the transaction (Release the lock)
		col.commit();

		// Search the Collection for a Row
		Query<Person> query = col.query("select * where name = 'name02'");

		// Fetch and update the searched Row
		RowSet<Person> rs = query.fetch(update);
		while (rs.hasNext()) {
			// Update the searched Row
			Person person1 = rs.next();
			person1.count += 1;
			rs.update(person1);

			System.out.println("Person: " +
					" name=" + person1.name +
					" status=" + person1.status +
					" count=" + person1.count +
					" lob=" + Arrays.toString(person1.lob));
		}

		// Commit the transaction
		col.commit();

		// Release the resource
		store.close();
	}

}