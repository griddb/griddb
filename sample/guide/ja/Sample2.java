package test;


import java.util.Date;
import java.util.Properties;

import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.RowKey;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.TimestampUtils;
import com.toshiba.mwcloud.gs.TimeUnit;


// 時系列データの登録と範囲取得
public class Sample2 {

	static class Point {
		@RowKey Date timestamp;
		boolean active;
		double voltage;
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

		// 時系列の作成 (既存の場合は取得のみ)
		TimeSeries<Point> ts = store.putTimeSeries("point01", Point.class);

		// 時系列要素のデータを用意
		Point point = new Point();
		point.active = false;
		point.voltage = 100;

		// 時系列要素の登録(グリッドストア側で時刻設定)
		ts.append(point);

		// 指定区間の時系列の取得: 6時間前から直近まで
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

		// リソースの解放
		store.close();
	}

}