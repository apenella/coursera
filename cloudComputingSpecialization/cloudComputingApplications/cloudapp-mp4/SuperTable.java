import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

public class SuperTable{

   public static void main(String[] args) throws IOException {

      // Instantiate Configuration class
	Configuration config = HBaseConfiguration.create();

      // Instaniate HBaseAdmin class
	HBaseAdmin admin = new HBaseAdmin(config);
     
      // Instantiate table descriptor class
	HTableDescriptor tablePowers = new HTableDescriptor(TableName.valueOf("powers"));

      // Add column families to table descriptor
	tablePowers.addFamily(new HColumnDescriptor("personal"));
	tablePowers.addFamily(new HColumnDescriptor("professional"));

      // Execute the table through admin
	admin.createTable(tablePowers);

      // Instantiating HTable class
	HTable hTable = new HTable(config, "powers");
     
      // Repeat these steps as many times as necessary

	      // Instantiating Put class
              // Hint: Accepts a row name
		Put prow1 = new Put(Bytes.toBytes("row1")); 
		Put prow2 = new Put(Bytes.toBytes("row2")); 
		Put prow3 = new Put(Bytes.toBytes("row3")); 

      	      // Add values using add() method
              // Hints: Accepts column family name, qualifier/row name ,value
		prow1.add(Bytes.toBytes("personal"),Bytes.toBytes("hero"),Bytes.toBytes("superman"));
		prow2.add(Bytes.toBytes("personal"),Bytes.toBytes("hero"),Bytes.toBytes("batman"));
		prow3.add(Bytes.toBytes("personal"),Bytes.toBytes("hero"),Bytes.toBytes("wolverine"));
		
		prow1.add(Bytes.toBytes("personal"),Bytes.toBytes("power"),Bytes.toBytes("strength"));
		prow2.add(Bytes.toBytes("personal"),Bytes.toBytes("power"),Bytes.toBytes("money"));
		prow3.add(Bytes.toBytes("personal"),Bytes.toBytes("power"),Bytes.toBytes("healing"));

		prow1.add(Bytes.toBytes("professional"),Bytes.toBytes("name"),Bytes.toBytes("clark"));
		prow2.add(Bytes.toBytes("professional"),Bytes.toBytes("name"),Bytes.toBytes("bruce"));
		prow3.add(Bytes.toBytes("professional"),Bytes.toBytes("name"),Bytes.toBytes("logan"));
		
		prow1.add(Bytes.toBytes("professional"),Bytes.toBytes("xp"),Bytes.toBytes("100"));
		prow2.add(Bytes.toBytes("professional"),Bytes.toBytes("xp"),Bytes.toBytes("50"));
		prow3.add(Bytes.toBytes("professional"),Bytes.toBytes("xp"),Bytes.toBytes("75"));
      // Save the table
 	hTable.put(prow1);	
 	hTable.put(prow2);	
 	hTable.put(prow3);
	
      // Close table
	hTable.close();

      // Instantiate the Scan class
	Scan scan = new Scan();
	hTable = new HTable(config, "powers");
     
      // Scan the required columns
	scan.addColumn(Bytes.toBytes("personal"),  Bytes.toBytes("hero"));

      // Get the scan result
	ResultScanner scanner= hTable.getScanner(scan);
      
      // Read values from scan result
      // Print scan result
	for( Result result = scanner.next(); result != null; result = scanner.next())

	System.out.println(result);
 
      // Close the scanner
  	scanner.close(); 
      // Htable closer
	hTable.close();
   }
}

