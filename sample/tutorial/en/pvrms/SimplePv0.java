package pvrms;

import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;

import com.opencsv.CSVReader;

import com.toshiba.mwcloud.gs.Collection;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.RowKey;

// Facility information
class Equip {
	@RowKey String id;
	String   name;
	//Blob     spec;	// For simplicity, spec information is not used.
}

public class SimplePv0 {

    /*
     * Load facility information from a CSV file.
     */
    public static void main(String[] args) throws GSException, ParseException, IOException {

        final String equipColName = "equipment_col";
        if (args.length != 5) {
                System.out.println("Usage:pvrms.SimplePv0 Addr Port ClusterName User Passwd ");
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
        String dataFileName = "equipName.csv";
        CSVReader reader = new CSVReader(new FileReader(dataFileName));
        String[] nextLine;

        // Create a Collection.
        Collection<String,Equip> equipCol = store.putCollection(equipColName, Equip.class);

        // Create indices for Columns.
        equipCol.createIndex("id");
        equipCol.createIndex("name");

        // Set autocommit mode to OFF.
        equipCol.setAutoCommit(false);

        // Set commit interval.
        Long commitInterval = (long) 1;

        // Store a value.
        Equip equip = new Equip();
        Long cnt = (long) 0;
        byte[] b = new byte[1];
        b[0] = 1;

        while ((nextLine = reader.readNext()) != null) {
            // Store facility information.
            equip.id		 = nextLine[0];
            equip.name		 = nextLine[1];
            equipCol.put(equip);
            cnt++;
            if(0 == cnt%commitInterval) {
                // Commit a transaction.
                equipCol.commit();
            }
        }
        // Commit a transaction.
        equipCol.commit();
        System.out.println("Container equip_col has been created. " + cnt + " rows have been put.");
        
        // Release resources.
        store.close();
        reader.close();
    }
}