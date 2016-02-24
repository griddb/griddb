package gsSample;


import java.util.Date;
import java.util.Properties;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.RowKey;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.TimeUnit;
import com.toshiba.mwcloud.gs.TimestampUtils;


// Storage and extraction of a specific range of time-series data
public class Sample2 {

	static class Point {
		@RowKey Date timestamp;
		boolean active;
		double voltage;
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

		// Create a TimeSeries (Only obtain the specified TimeSeries if it already exists)
		TimeSeries<Point> ts = store.putTimeSeries("point01", Point.class);

		// Prepare time-series element data
		Point point = new Point();
		point.active = false;
		point.voltage = 100;

		// Store the time-series element (GridStore sets its timestamp)
		ts.append(point);

		// Extract the specified range of time-series elements: last six hours
		Date now = TimestampUtils.current();
		Date before = TimestampUtils.add(now, -6, TimeUnit.HOUR);

		RowSet<Point> rs = ts.query(before, now).fetch();

		while (rs.hasNext()) {
			point = rs.next();

			System.out.println(
					"Time=" + TimestampUtils.format(point.timestamp) +
					" Active=" + point.active +
					" Voltage=" + point.voltage);
		}

		// Release the resource
		store.close();
	}

}