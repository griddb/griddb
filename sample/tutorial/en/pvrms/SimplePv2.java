package pvrms;

import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import com.opencsv.CSVReader;

import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.RowKey;
import com.toshiba.mwcloud.gs.TimeSeries;

// Sensor data
class Point {
    @RowKey Date time;
    double  value;
    String  status;
}

public class SimplePv2 {

    /*
     * Load time-series data from a CSV file.
     */
    public static void main(String[] args) throws ParseException, IOException {

        if (args.length != 5) {
                System.out.println("Usage:pvrms.SimplePv2 Addr Port ClusterName User Passwd ");
                System.exit(1);
        }

        // Get a GridStore instance.
        Properties props = new Properties();
        props.setProperty("notificationAddress", args[0]);
        props.setProperty("notificationPort", args[1]);
        props.setProperty("clusterName", args[2]);
        props.setProperty("user", args[3]);
        props.setProperty("password", args[4]);

        GridStore store = GridStoreFactory.getInstance().getGridStore(props);

        // Read a CSV file.
        String dataFileName = "sensorHistory.csv";
        CSVReader reader = new CSVReader(new FileReader(dataFileName));
        String[] nextLine;
        nextLine = reader.readNext();

        // Read a sensor ID and create a TimeSeries.
        String[] tsNameArray = new String[nextLine.length];

        for(int j = 0; j < nextLine.length; j++) {
            tsNameArray[j] = nextLine[j];
            store.putTimeSeries(tsNameArray[j], Point.class);
        }

        // Store a value in each TimeSeries.
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Point point             = new Point();
        Long cnt = (long) 0;
        while ((nextLine = reader.readNext()) != null) {
            String dateS     = nextLine[0];
            String timeS     = nextLine[1];
            String datetimeS = dateS + " " + timeS ;
            Date   date      = format.parse(datetimeS);

            for(int i = 0, j = 2; j < nextLine.length; i++, j+=2) {
                TimeSeries<Point> ts = store.getTimeSeries(tsNameArray[i], Point.class);

                point.time   = date;
                point.value  = Double.valueOf(nextLine[j]);
                point.status = nextLine[j+1];

                ts.put(date,point);
            }
            cnt++;
        }
        System.out.println(tsNameArray.length + " timeseries containers have been created. " + cnt + " rows have been put.");

        // Release resources.
        store.close();
        reader.close();
    }
}