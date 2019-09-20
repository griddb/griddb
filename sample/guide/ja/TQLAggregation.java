import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.toshiba.mwcloud.gs.AggregationResult;
import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.Row;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.TimeSeries;

public class TQLAggregation {

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

			//===============================================
			// コンテナを作成する
			//===============================================
			createContainerPutRow(store);


			//===============================================
			// TQLの集計演算
			//===============================================
			// (1) Containerオブジェクトの取得
			Container<?, Row> container = store.getContainer("SampleJava_TQLAggregation");
			if ( container == null ){
				throw new Exception("Container not found.");
			}
			{
				//------------------------------------
				// 最大値 MAX
				//------------------------------------
				// (1)TQLで集計演算の実行
				Query<AggregationResult> query = container.query("SELECT MAX(count)", AggregationResult.class);
				RowSet<AggregationResult> rs = query.fetch();

				// (2)結果を取得
				if ( rs.hasNext() ){
					AggregationResult result = rs.next();
					long value = result.getLong();
					System.out.println("max = "+ value);
				}
				rs.close();
				query.close();
			}
			{
				//------------------------------------
				// 最小値  MIN
				//------------------------------------
				// TQLで集計演算の実行
				Query<AggregationResult> query = container.query("SELECT MIN(count)", AggregationResult.class);
				RowSet<AggregationResult> rs = query.fetch();

				// 結果を取得
				if ( rs.hasNext() ){
					AggregationResult result = rs.next();
					long value = result.getLong();
					System.out.println("min = "+ value);
				}
				rs.close();
				query.close();
			}
			{
				//------------------------------------
				// 個数 COUNT
				//------------------------------------
				// TQLで集計演算の実行
				Query<AggregationResult> query = container.query("SELECT COUNT(*)", AggregationResult.class);
				RowSet<AggregationResult> rs = query.fetch();

				// 結果を取得
				if ( rs.hasNext() ){
					AggregationResult result = rs.next();
					long value = result.getLong();
					System.out.println("count = "+ value);
				}
				rs.close();
				query.close();
			}
			{
				//------------------------------------
				// 合計 SUM
				//------------------------------------
				// TQLで集計演算の実行
				Query<AggregationResult> query = container.query("SELECT SUM(count)", AggregationResult.class);
				RowSet<AggregationResult> rs = query.fetch();

				// 結果を取得
				if ( rs.hasNext() ){
					AggregationResult result = rs.next();
					long value = result.getLong();
					System.out.println("sum = "+ value);
				}
				rs.close();
				query.close();
			}
			{
				//------------------------------------
				// 平均 AVG
				//------------------------------------
				// TQLで集計演算の実行
				Query<AggregationResult> query = container.query("SELECT AVG(count)", AggregationResult.class);
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
			{
				//------------------------------------
				// 分散値 VARIANCE
				//------------------------------------
				// TQLで集計演算の実行
				Query<AggregationResult> query = container.query("SELECT VARIANCE(count)", AggregationResult.class);
				RowSet<AggregationResult> rs = query.fetch();

				// 結果を取得
				if ( rs.hasNext() ){
					AggregationResult result = rs.next();
					double value = result.getDouble();
					System.out.println("variance = "+ value);
				}
				rs.close();
				query.close();
			}
			{
				//------------------------------------
				// 標準偏差 STDDEV
				//------------------------------------
				// TQLで集計演算の実行
				Query<AggregationResult> query = container.query("SELECT STDDEV(count)", AggregationResult.class);
				RowSet<AggregationResult> rs = query.fetch();

				// 結果を取得
				if ( rs.hasNext() ){
					AggregationResult result = rs.next();
					double value = result.getDouble();
					System.out.println("stddev = "+ value);
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

	private static void createContainerPutRow(GridStore store) throws Exception {
		{
			// コンテナを作成する
			ContainerInfo containerInfo = new ContainerInfo();
			List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
			columnList.add(new ColumnInfo("id", GSType.INTEGER));
			columnList.add(new ColumnInfo("productName", GSType.STRING));
			columnList.add(new ColumnInfo("count", GSType.INTEGER));
			containerInfo.setColumnInfoList(columnList);
			containerInfo.setRowKeyAssigned(true);

			Collection<Void, Row> collection = store.putCollection("SampleJava_TQLAggregation", containerInfo, false);
			System.out.println("Create Collection name=SampleJava_TQLAggregation");


			// ロウを登録する
			String[] nameList = {"notebook PC", "desktop PC", "keybord", "mouse", "printer"};
			int[] numberList = {108, 72, 25, 45, 62};

			List<Row> rowList = new ArrayList<Row>();
			for ( int i = 0; i < nameList.length; i++ ){
				Row row = collection.createRow();
				row.setInteger(0, i);
				row.setString(1, nameList[i]);
				row.setInteger(2, numberList[i]);
				rowList.add(row);
			}
			collection.put(rowList);
		}
	}
}