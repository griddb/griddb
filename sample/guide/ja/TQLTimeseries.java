import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import com.toshiba.mwcloud.gs.AggregationResult;
import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.ContainerType;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.TimestampUtils;

public class TQLTimeseries {

	public static void main(String[] args){
		try {
			//===============================================
			// クラスタに接続する
			//===============================================
			// 接続情報を指定する (マルチキャスト方式)
			Properties prop = new Properties();
			prop.setProperty("notificationAddress", "239.0.0.1");
			prop.setProperty("notificationPort", "31999");
			prop.setProperty("clusterName", "myCluster");
			prop.setProperty("database", "public");
			prop.setProperty("user", "admin");
			prop.setProperty("password", "admin");
			prop.setProperty("applicationName", "SampleJava");

			// GridStoreオブジェクトを生成する
			GridStore store = GridStoreFactory.getInstance().getGridStore(prop);
			// コンテナ作成や取得などの操作を行うと、クラスタに接続される
			store.getContainer("dummyContainer");

			String containerName = "SampleJava_TQLTimeseries";
			{
				Container<?, Row> container = store.getContainer(containerName);
				if ( container != null ){
					store.dropContainer(containerName);
				}
			}

			//===============================================
			// 時系列コンテナ作成する
			//===============================================
			// (1)コンテナ情報オブジェクトを生成
			ContainerInfo containerInfo = new ContainerInfo();

			// (2)カラムの名前やデータ型をカラム情報オブジェクトにセット
			List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
			columnList.add(new ColumnInfo("date", GSType.TIMESTAMP));
			columnList.add(new ColumnInfo("value1", GSType.INTEGER));
			columnList.add(new ColumnInfo("value2", GSType.DOUBLE));
			containerInfo.setColumnInfoList(columnList);

			// (3)ロウキーを設定
			containerInfo.setRowKeyAssigned(true);

			containerInfo.setType(ContainerType.TIME_SERIES);
			// TIME_AVGは、コレクションに対して実行するとエラーになる
			// [152004:TQ_CONSTRAINT_AGGREGATION_ABUSE] Aggregation for timeSeries is used to collection.

			// (4)時系列コンテナ作成
			Container<?, Row> container = store.putContainer(containerName, containerInfo, false);

			System.out.println("Create Timeseries name="+containerName);

			//===============================================
			// ロウを登録する
			//===============================================
			{
				// 登録するデータ
				String[] dateList = {"2018/12/01 10:00:00.000+0000",
						"2018/12/01 10:10:00.000+0000",
						"2018/12/01 10:20:00.000+0000",
				"2018/12/01 10:40:00.000+0000" };
				int[] value1List = {1, 3, 2, 4};
				double[] value2List = { 10.3, 5.7, 8.2, 4.5 };

				// 日付の変換フォーマット
				SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss.SSSZ");

				List<Row> rowList = new ArrayList<Row>();
				for ( int i = 0; i < dateList.length; i++ ){
					Row row = container.createRow();
					row.setTimestamp(0, format.parse(dateList[i]));
					row.setInteger(1, value1List[i]);
					row.setDouble(2, value2List[i]);
					rowList.add(row);
				}

				// (3)複数のロウの登録
				container.put(rowList);

				System.out.println("Put Rows num=" + dateList.length);
			}
			{
				//------------------------------------
				// 平均 AVG
				//------------------------------------
				// TQLで集計演算の実行
				Query<AggregationResult> query = container.query("SELECT AVG(value1)", AggregationResult.class);
				RowSet<AggregationResult> rs = query.fetch();

				// 結果を取得
				if ( rs.hasNext() ){
					AggregationResult result = rs.next();
					double value = result.getDouble();
					System.out.println("avg = "+ value);
				}
				rs.close();
				query.close();
			}
			//===============================================
			// 時系列特有の集計演算
			//===============================================
			{
				//------------------------------------
				// 重み付き平均 TIME_AVG
				//------------------------------------
				// TQLで演算の実行
				Query<AggregationResult> query = container.query("SELECT TIME_AVG(value1)", AggregationResult.class);
				RowSet<AggregationResult> rs = query.fetch();

				// 結果を取得
				if ( rs.hasNext() ){
					AggregationResult result = rs.next();
					double value = result.getDouble();
					System.out.println("TIME_AVG = "+ value);
				}
				rs.close();
				query.close();
			}

			//===============================================
			// 時系列特有の選択演算
			//===============================================
			{
				//------------------------------------
				// TIME_NEXT
				//------------------------------------
				// TQLで演算の実行
				Query<Row> query = container.query("SELECT TIME_NEXT(*, TIMESTAMP('2018-12-01T10:10:00.000Z'))");
				RowSet<Row> rs = query.fetch();

				// 結果を取得(ロウはひとつしか返らない)
				if ( rs.hasNext() ){
					Row row = rs.next();
					Date date = row.getTimestamp(0);
					int value1 = row.getInteger(1);
					double value2 = row.getDouble(2);
					System.out.println("TIME_NEXT row date=" + TimestampUtils.format(date) + ", value1=" + value1 + ", value2=" + value2);
				}
				rs.close();
				query.close();
			}

			//===============================================
			// 時系列特有の補間演算
			//===============================================
			{
				//------------------------------------
				// TIME_INTERPOLATED
				//------------------------------------
				// TQLで演算の実行
				Query<Row> query = container.query("SELECT TIME_INTERPOLATED(value1, TIMESTAMP('2018-12-01T10:30:00.000Z'))");
				RowSet<Row> rs = query.fetch();

				// 結果を取得(ロウはひとつしか返らない)
				if ( rs.hasNext() ){
					Row row = rs.next();
					Date date = row.getTimestamp(0);
					int value1 = row.getInteger(1);
					double value2 = row.getDouble(2);
					System.out.println("TIME_INTERPOLATED row date=" + date + ", value1=" + value1 + ", value2=" + value2);
				}
				rs.close();
				query.close();
			}


			//===============================================
			// ロウを登録する
			//===============================================
			{
				// 登録するデータ
				String[] dateList = {"2018/12/01 10:45:00.000+0000",
						"2018/12/01 10:50:00.000+0000",
				"2018/12/01 11:00:00.000+0000"};
				int[] value1List = {1, 4, 3};
				double[] value2List = { 6.3, 18.9, 3.6 };

				// 日付の変換フォーマット
				SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss.SSSZ");

				List<Row> rowList = new ArrayList<Row>();
				for ( int i = 0; i < dateList.length; i++ ){
					Row row = container.createRow();
					row.setTimestamp(0, format.parse(dateList[i]));
					row.setInteger(1, value1List[i]);
					row.setDouble(2, value2List[i]);
					rowList.add(row);
				}

				// (3)複数のロウの登録
				container.put(rowList);

				System.out.println("Put Rows num=" + dateList.length);

			}

			//===============================================
			// 時系列特有の補間演算
			//===============================================
			{
				//------------------------------------
				// TIME_SAMPLING
				//------------------------------------
				// TQLで演算の実行
				Query<Row> query = container.query("SELECT TIME_SAMPLING(value1, TIMESTAMP('2018-12-01T10:00:00.000Z'),TIMESTAMP('2018-12-01T11:00:00.000Z'), 15, MINUTE)");
				RowSet<Row> rs = query.fetch();

				// 結果を取得
				while ( rs.hasNext() ){
					Row row = rs.next();
					Date date = row.getTimestamp(0);
					int value1 = row.getInteger(1);
					double value2 = row.getDouble(2);
					System.out.println("TIME_SAMPLING row date=" + date + ", value1=" + value1 + ", value2=" + value2);
				}
				rs.close();
				query.close();
			}


			//===============================================
			// 終了処理
			//===============================================
			container.close();
			store.close();
			System.out.println("success!");

		} catch ( Exception e ){
			e.printStackTrace();
		}
	}
}