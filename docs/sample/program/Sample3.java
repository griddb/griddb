package gsSample;


import java.util.Date;
import java.util.Properties;

import com.toshiba.mwcloud.gs.Aggregation;
import com.toshiba.mwcloud.gs.AggregationResult;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.RowKey;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.TimeOperator;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.TimeUnit;
import com.toshiba.mwcloud.gs.TimestampUtils;


// Search and aggregation of time-series data
public class Sample3 {

	static class Point {
		@RowKey Date timestamp;
		boolean active;
		double voltage;
	}

	public static void main(String[] args) throws GSException {

		// Lower the consistency level because of read-only operation (default: IMMEDIATE)
		Properties props = new Properties();
		props.setProperty("notificationAddress", args[0]);
		props.setProperty("notificationPort", args[1]);
		props.setProperty("clusterName", args[2]);
		props.setProperty("user", args[3]);
		props.setProperty("password", args[4]);
		props.setProperty("consistency", "EVENTUAL");

		// Get a GridStore instance
		GridStore store = GridStoreFactory.getInstance().getGridStore(props);

		// Obtain a TimeSeries
		// ※ Use the Point class as in Sample 2
		TimeSeries<Point> ts = store.getTimeSeries("point01", Point.class);

		// Search for the locations whose voltage is not lower than a reference value, though not in action.
		Query<Point> query = ts.query(
				"select * from point01" +
				" where not active and voltage > 50");
		RowSet<Point> rs = query.fetch();

		while (rs.hasNext()) {
			// Examine each abnormal point

			Point hotPoint = rs.next();
			Date hot = hotPoint.timestamp;

			// Obtain the data around ten minutes before
			Date start = TimestampUtils.add(hot, -10, TimeUnit.MINUTE);
			Point startPoint = ts.get(start, TimeOperator.NEXT);

			// Calculate the average of the data for 20 minutes around the point
			Date end = TimestampUtils.add(hot, 10, TimeUnit.MINUTE);
			AggregationResult avg =
				ts.aggregate(start, end, "voltage", Aggregation.AVERAGE);

			System.out.println(
					"[Alert] " + TimestampUtils.format(hot) +
					" start=" + startPoint.voltage +
					" hot=" + hotPoint.voltage +
					" avg=" + avg.getDouble());
		}

		// Release the resource
		store.close();
	}

}