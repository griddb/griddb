package pvrms;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.TimeUnit;
import com.toshiba.mwcloud.gs.TimestampUtils;

public class SimplePv3 {

    /*
     *  Displays facility information related to serious alerts occurring within
     *  24 hours and time-series data just before each alert.
     */
    public static void main(String[] args) throws GSException, ParseException, IOException {

        final String alertColName = "alert_col";
        final String equipColName = "equipment_col";

        if (args.length != 5) {
            System.out.println("Usage:pvrms.SimplePv3 Addr Port ClusterName User Passwd ");
            System.exit(1);
        }

        // Get a GridStore instance.
        Properties props = new Properties();
        props.setProperty("notificationAddress", args[0]);
        props.setProperty("notificationPort", args[1]);
        props.setProperty("clusterName", args[2]);
        props.setProperty("user", args[3]);
        props.setProperty("password", args[4]);
        GridStore	store = GridStoreFactory.getInstance().getGridStore(props);
        Collection<Long,Alert> alertCol = store.getCollection(alertColName, Alert.class);

        // Set alert monitoring time.
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS",Locale.JAPAN);
        Date tm1=sf.parse("2015-01-02 08:00:00.000");

        System.out.println("Search for serious alerts (level > 3) within 24 hours from alert_col.");

        //String from =    TimestampUtils.format( TimestampUtils.add(new Date(System.currentTimeMillis()), -24, TimeUnit.HOUR) );
        String from =    TimestampUtils.format( TimestampUtils.add(tm1, -24, TimeUnit.HOUR) );
        Query<Alert> alertQuery = alertCol.query( "select * where level > 3 and " +
                                                     "timestamp > " + "TIMESTAMP('" + from + "')" );

        System.out.println("select * where level > 3 and " +
                              "timestamp > " + "TIMESTAMP('" + from + "')");
        RowSet<Alert> alertRs = alertQuery.fetch();

        System.out.println("Display facility information related to a sensor occurring a serious alert and time-series data just before the alert.");
        System.out.println("    Alert count: " + alertRs.size());
        while( alertRs.hasNext() ) {
            Alert seriousAlert = alertRs.next();

            // Obtain a facility ID and a sensor type.
            String sensorId = seriousAlert.sensorId;
            String sensorType = sensorId.substring(sensorId.indexOf("_")+1);

            // Obtain facility information.
            Collection<String,Equip> equipCol = store.getCollection(equipColName, Equip.class);
            Equip equip = equipCol.get(sensorId);

            System.out.println("[Equipment] " + equip.name  + " (sensor) "+ sensorType);
            System.out.println("[Detail] " + seriousAlert.detail);

            // Search for time-series data just before the alert.
            String tsName = seriousAlert.sensorId;
            TimeSeries<Point> ts = store.getTimeSeries(tsName, Point.class);
            Date endDate   = seriousAlert.timestamp;
            Date startDate = TimestampUtils.add(seriousAlert.timestamp, -10, TimeUnit.MINUTE);
            RowSet<Point> rowSet =  ts.query(startDate, endDate).fetch();
            while (rowSet.hasNext()) {
                Point ret = rowSet.next();
                System.out.println(
                        "[Result] " +sf.format(ret.time) +
                        " " + ret.value + " " + ret.status);
            }
            System.out.println("");
        }

        // Release a resource.
        store.close();
    }
}