package pvrms;

import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import com.opencsv.CSVReader;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.RowKey;

// Alert information
class Alert {
    @RowKey Long id;
    Date    timestamp;
    String  sensorId;
    int     level;
    String  detail;
}

public class SimplePv1 {

    /*
     * Load alert data from a CSV file.
     */
    public static void main(String[] args) throws GSException, ParseException, IOException {

        final String alertColName = "alert_col";
        if (args.length != 5) {
                System.out.println("Usage:pvrms.SimplePv1 Addr Port ClusterName User Passwd ");
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

        // Read a CSV file.
        String dataFileName = "alarmHistory.csv";
        CSVReader reader = new CSVReader(new FileReader(dataFileName));
        String[] nextLine;

        // Create a Collection.
        Collection<Long,Alert> alertCol = store.putCollection(alertColName, Alert.class);

        // Create indices for columns.
        alertCol.createIndex("timestamp");
        alertCol.createIndex("level");

        // Set autocommit mode to OFF.
        alertCol.setAutoCommit(false);

        // Set commit interval.
        Long commitInterval = (long) 24;

        // Store a value.
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Alert alert = new Alert();
        Long cnt = (long) 0;
        while ((nextLine = reader.readNext()) != null) {

            String dateS     = nextLine[0];
            String timeS     = nextLine[1];
            String datetimeS = dateS + " " + timeS ;
            Date   date      = format.parse(datetimeS);

            alert.id         = ++cnt;
            alert.timestamp  = date;
            alert.sensorId   = nextLine[2];
            alert.level      = Integer.valueOf(nextLine[3]);
            alert.detail     = nextLine[4];

            alertCol.put(alert);

            if(0 == cnt%commitInterval) {
                // Commit a transaction.
                alertCol.commit();
            }
        }
        // Commit a transaction.
        alertCol.commit();
        System.out.println("Container alert_col has been created. " + cnt + " rows have been put.");

        // Release resources.
        store.close();
        reader.close();
    }
}