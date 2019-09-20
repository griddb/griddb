package test;


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
import com.toshiba.mwcloud.gs.TimestampUtils;
import com.toshiba.mwcloud.gs.TimeUnit;


// 時系列データの検索と集計
public class Sample3 {

	static class Point {
		@RowKey Date timestamp;
		boolean active;
		double voltage;
	}

	public static void main(String[] args) throws GSException {

		// 読み取りのみなので、一貫性レベルを緩和(デフォルトはIMMEDIATE)
		Properties props = new Properties();
		props.setProperty("notificationAddress", args[0]);
		props.setProperty("notificationPort", args[1]);
		props.setProperty("clusterName", args[2]);
		props.setProperty("user", args[3]);
		props.setProperty("password", args[4]);
		props.setProperty("consistency", "EVENTUAL");

		// GridStoreインスタンスの取得
		GridStore store = GridStoreFactory.getInstance().getGridStore(props);

		// 時系列の取得
		// ※Sample2と同じPointクラスを使用
		TimeSeries<Point> ts = store.getTimeSeries("point01", Point.class);

		// 停止中にもかかわらず電圧が基準値以上の箇所を検索
		Query<Point> query = ts.query(
				"select * from point01" +
				" where not active and voltage > 50");
		RowSet<Point> rs = query.fetch();

		while (rs.hasNext()) {
			// 各異常ポイントについて調査

			Point hotPoint = rs.next();
			Date hot = hotPoint.timestamp;

			// 10分前付近のデータを取得
			Date start = TimestampUtils.add(hot, -10, TimeUnit.MINUTE);
			Point startPoint = ts.get(start, TimeOperator.NEXT);

			// 前後10分間の平均値を計算
			Date end = TimestampUtils.add(hot, 10, TimeUnit.MINUTE);
			AggregationResult avg = ts.aggregate(
					start, end, "voltage", Aggregation.AVERAGE);

			System.out.println(
					"[Alert] " + TimestampUtils.format(hot) +
					" start=" + startPoint.voltage +
					" hot=" + hotPoint.voltage +
					" avg=" + avg.getDouble());
		}

		// リソースの解放
		store.close();
	}

}