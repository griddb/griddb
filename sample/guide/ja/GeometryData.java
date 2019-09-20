import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.Geometry;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Row;

public class GeometryData {

	public static void main(String[] args){
		try {
			//===============================================
			// クラスタに接続する
			//===============================================
			// 接続情報を指定
			Properties prop = new Properties();
			prop.setProperty("notificationAddress", "239.0.0.1");
			prop.setProperty("notificationPort", "31999");
			prop.setProperty("clusterName", "myCluster");
			prop.setProperty("database", "public");
			prop.setProperty("user", "admin");
			prop.setProperty("password", "admin");
			prop.setProperty("applicationName", "SampleJava");

			// GridStoreオブジェクトを生成
			GridStore store = GridStoreFactory.getInstance().getGridStore(prop);
			// コンテナ作成や取得などの操作を行うと、クラスタに接続される
			store.getContainer("dummyContainer");


			//===============================================
			// コレクションを作成する
			//===============================================
			ContainerInfo containerInfo = new ContainerInfo();
			List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
			columnList.add(new ColumnInfo("id", GSType.INTEGER));
			columnList.add(new ColumnInfo("geometry", GSType.GEOMETRY));
			containerInfo.setColumnInfoList(columnList);
			containerInfo.setRowKeyAssigned(true);

			Collection<Integer, Row> container = store.putCollection("SampleJava_GeometryData", containerInfo, false);
			System.out.println("Create Collection name=SampleJava_GeometryData");

			//===============================================
			// 空間型のデータを登録する
			//===============================================
			{
				// (1)空間型のデータ
				Geometry geometry = Geometry.valueOf("POINT(2 3)");

				// (2)ロウに空間型のデータをセットする
				Row row = container.createRow();
				row.setInteger(0, 0);
				row.setGeometry(1, geometry);

				// (3)ロウを登録する
				container.put(row);

				System.out.println("Put Row (Geometry)");
			}

			//===============================================
			// 空間型のデータを取得する
			//===============================================
			{
				// (1)ロウを取得
				Row row = container.get(0);

				// (2)ロウから配列型データを取得
				Geometry geometry = row.getGeometry(1);

				System.out.println("Get Row (Geometry) geometry=" + geometry);
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