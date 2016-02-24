package gsSample;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.toshiba.mwcloud.gs.AggregationResult;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowSet;

// Bulk search(TQL) for many containes
//     
// It is necessary to carry this program out after having executed SampleMultiPut program
public abstract class SampleFetchAll {

	public static void main(String[] args)  {
		GridStore store = null;

		if (args.length != 6){
			System.out.println("Usage:gsSample/SampleFetchAll Addr Port ClusterName UserName Password ContaierCount");
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
				Container<Object, Row> container =store.getContainer("DUMMY");
				if (container != null) {
					container.close();
				}

			} catch (Exception e) {
				System.out.println("An error occurred when connecting to the server.");
				e.printStackTrace();
				System.exit(1);
			}

			try{
				List<Query<?>> queryList = new ArrayList<Query<?>>();
				for ( int i = 0; i <containerCount; i++ ){
					Container<Object, Row> container = store.getContainer("point"+i);
					Query<?> query = container.query("select MAX_ROWS(voltage)");
					queryList.add(query);
					query = container.query("select MIN_ROWS(voltage)");
					queryList.add(query);
					query = container.query("select MAX(voltage)",AggregationResult.class);
					queryList.add(query);
					query = container.query("select MIN(voltage)",AggregationResult.class);
					queryList.add(query);
					query = container.query("select AVG(voltage)",AggregationResult.class);
					queryList.add(query);
				}
				store.fetchAll(queryList);
				int i=0;
				for (Query<?> query : queryList) {
					final RowSet<?> rowSet = query.getRowSet();
					switch (i%5) {
					case 0:
						int pname=i/5;
						System.out.println("**** point"+pname+" ****");
						System.out.println("voltage MAX_ROWS ResultCount"+":"+rowSet.size());
						while (rowSet.hasNext()) {
							Row row = (Row) rowSet.next();
							System.out.println((row.getValue(0))+" "+(row.getValue(1))+" " +(row.getValue(2)));
						}
						break;
					case 1:
						System.out.println("voltage MIN_ROWS ResultCount"+":"+rowSet.size());
						while (rowSet.hasNext()) {
							Row row = (Row) rowSet.next();
							System.out.println((row.getValue(0))+" "+(row.getValue(1))+" " +(row.getValue(2)));
						}
						break;
					case 2:
						System.out.println( "voltage MAX="+((AggregationResult) rowSet.next()).getDouble());
						break;
					case 3:
						System.out.println( "voltage MAX="+((AggregationResult) rowSet.next()).getDouble());
						break;
					case 4:
						System.out.println( "voltage MAX="+((AggregationResult) rowSet.next()).getDouble());
						break;
					}
					i++;
				}
			} catch (Exception e) {

				System.out.println("An error occurred when searching.");
				e.printStackTrace();
				System.exit(1);
			}

		} finally {
			// Release the resource
			if ( store != null ){
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

