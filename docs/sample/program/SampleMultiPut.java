package gsSample;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.IndexType;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowKey;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.TimeUnit;
import com.toshiba.mwcloud.gs.TimestampUtils;

// Bulk load for many containers

public class SampleMultiPut {

	static class Point {
		@RowKey Date timestamp;
		boolean active;
		double voltage;
	}

	public static void main(String[] args) {
		GridStore store = null;

		if (args.length != 7) {
			System.out.println("Usage:gsSample.SampleMultiPut Addr Port ClusterName UserName Password ContaierCount RowCount");
			System.exit(1);
		}

		try{
			Properties props = new Properties();
			props.setProperty("notificationAddress", args[0]);
			props.setProperty("notificationPort", args[1]);
			props.setProperty("clusterName", args[2]);
			props.setProperty("user", args[3]);
			props.setProperty("password", args[4]);
			props.setProperty("transactionTimeout","30");
			props.setProperty("failoverTimeout","30");
			int containerCount = Integer.parseInt(args[5]);
			int rowCount = Integer.parseInt(args[6]);

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

			List<String> containerNameList = new ArrayList<String>();
			for (int i = 0 ; i < containerCount; i++) {
				String name = "point"+i;
				containerNameList.add(name);
				TimeSeries<Point> ts;
				try {
					ts = store.putTimeSeries(name, Point.class);
					ts.createIndex("voltage", IndexType.TREE);
				} catch (GSException e) {
					System.out.println("An error occurred when creating the container.");
					e.printStackTrace();
					System.exit(1);
				}
			}

			final Map<String, List<Row>> rowListMap = new HashMap<String, List<Row>>();
			Random rnd = new Random();

			for (String containerName : containerNameList) {
				final List<Row> rowList = new ArrayList<Row>();
				ContainerInfo containerInfo;
				try {
					containerInfo = store.getContainerInfo(containerName);
					System.out.println(containerName);
					SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
					Date tm1=sf.parse("2015-04-27 19:00:00.000");
					for (int j = 0; j < rowCount; j++) {
						Row row = store.createRow(containerInfo);
						row.setTimestamp(0,TimestampUtils.add(tm1, j, TimeUnit.MINUTE));
						row.setBool(1, false);
						row.setDouble(2, rnd.nextInt(10000));
						rowList.add(row);
					}
					rowListMap.put(containerName, rowList);

				} catch (GSException e) {
					System.out.println("An error occurred by the making of row data.");
					e.printStackTrace();
					System.exit(1);
				} catch (ParseException e) {
					System.out.println("The format of the date had an error");
					e.printStackTrace();
					System.exit(1);
				}
			}
			try{
				store.multiPut(rowListMap);
			} catch (Exception e) {
				System.out.println("An error occurred when updating.");
				System.exit(1);
			}
		} finally {
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

