package gsSample;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowKeyPredicate;

// Bulk search(get) for many containes
//     
// It is necessary to carry this program out after having executed SampleMultiPut program
public class SampleMultiGet {

	public static void main(String[] args) {
		GridStore store = null;

		if (args.length != 6) {
			System.out.println("Usage:gsSample.SampleMultiGet Addr Port ClusterName UserName Password ContaierCount");
			System.exit(1);
		}

		try {
			Properties props = new Properties();
			props.setProperty("notificationAddress", args[0]);
			props.setProperty("notificationPort", args[1]);
			props.setProperty("clusterName", args[2]);
			props.setProperty("user", args[3]);
			props.setProperty("password", args[4]);
			props.setProperty("transactionTimeout","30");
			props.setProperty("failoverTimeout","30");
			int containerCount=Integer.parseInt(args[5]);

			try{
				store = GridStoreFactory.getInstance().getGridStore(props);
				Container<Object, Row> container = store.getContainer("DUMMY");
				if (container != null) {
					container.close();
				}

			} catch (Exception e) {
				System.out.println("An error occurred when connecting to the server.");
				e.printStackTrace();
				System.exit(1);
			}

			try{
				Map<String, RowKeyPredicate<?>> queryList = new HashMap<String, RowKeyPredicate<?>>();
				for (int i = 0; i < containerCount; i++){
					String name = "point"+i;
					RowKeyPredicate<Date> predicate = RowKeyPredicate.create(Date.class);
					SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
					predicate.setStart(sf.parse("2015-04-27 19:00:00.000"));
					predicate.setFinish(sf.parse("2015-04-27 19:20:00.000"));

					queryList.put(name, predicate);
				}

				Map<String, List<Row>> resultMap = store.multiGet(queryList);
				for (Map.Entry<String, List<Row>> entry : resultMap.entrySet()) {
					System.out.println("**** "+entry.getKey()+" ****");
					for (Row row : entry.getValue()) {
						System.out.println((row.getValue(0)) + " " + (row.getValue(1)) + " " + (row.getValue(2)));
					}
				}

			} catch (Exception e) {
				System.out.println("An error occurred when searching.");
				e.printStackTrace();
				System.exit(1);
			}


		} finally {
			// Release the resource
			if (store != null) {
				try {
					store.close();
				} catch (GSException e) {
					System.out.println("An error occurred when releasing the recsource.");
					e.printStackTrace();
				}
			}
		}
	}

}

