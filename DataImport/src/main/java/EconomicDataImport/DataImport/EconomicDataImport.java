package EconomicDataImport.DataImport;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;

/**
 * 
 * Modified by Yu Hu <yhu@insidesales.com>
 * Updated by Lucas Song <ysong@insidesales.com>
 *
 */
public abstract class EconomicDataImport {

	private List<Date> StartEnd = null; // two dates in, start and end dates
	private SimpleDateFormat sdf = null;
	private static Configuration conf = null;
	private List<Put> puts;
	static Logger log = Logger.getLogger(EconomicDataImport.class.getName());

	public EconomicDataImport() {
		StartEnd = new ArrayList<Date>();
		sdf = new SimpleDateFormat("yyyy-MM-dd");
		puts = new ArrayList<Put>();
		conf = getConfiguration();
	}

	// Singleton, only need to initialize once.
	public static Configuration getConfiguration() {
		if(conf == null) {
			conf = new Configuration();
			/* 
			 * Run this code on the local machine, then uncomment the code below
			 
	        conf.clear();
	        conf.set("hbase.zookeeper.quorum", "10.0.5.50"); // hadoop0
	        conf.set("hbase.zookeeper.property.clientPort", "2181");
	        conf.set("hbase.master", "10.0.5.50");
	        */
		}
		
		return conf;
	}
	
	/**
	 * Compare the date with current started date and the ended date to
	 * determine them for function doCPI()
	 *         
	 * @param date: the date on the left column of .csv file
	 */
	private void initializeStartAndEndDates(String date) {
		try {
			if(date.equalsIgnoreCase("Date"))
				return;
			
			Date d = sdf.parse(date);
			
			if(StartEnd.isEmpty()) {
				StartEnd.add(d);
			}
			else {
				// if the new date is before the existing date
				if(d.before(StartEnd.get(0))) {
					Date tmp = StartEnd.get(0); // the existing date is older
					StartEnd.set(0, d);
					if(StartEnd.size() == 1) // if only one date in the array
						StartEnd.add(tmp);
				}
				// if the new date is after the existing date
				else if(d.after(StartEnd.get(0))) {
					if(StartEnd.size() == 1) // if only one date in the array
						StartEnd.add(d);
					else if(d.after(StartEnd.get(1))) // if the new date is after the second date
						StartEnd.set(1, d);  // update the date
				}
			}
		}catch(ParseException e) {
			System.out.println("Error:\n" + date);// + "\n" + 
			log.error("EconomicDataImport class, Date is in String, cannot connvert it into a Date object!\n" +
			e.toString());
			e.printStackTrace();
		}
	}
	
	private Put createPut(String rowkey, String columnFamily, String qualifier, String value) {
		Put putData = new Put(Bytes.toBytes(rowkey));
		putData.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		
		return putData; 
	}
	
	/**
	 * @return the started date in the .csv file
	 */
	public Date getStartDate() {
		
		return StartEnd.get(0);
	}
	
	/**
	 * @return the ended date in the .csv file
	 */
	public Date getStopDate() {
		return StartEnd.get(1);
	}
	
	/*
	 * Build the row based on the qualifiers
	 * @param rowkey
	 * @param column Family name
	 * @param content, is a Map, the key is a qualifier and value is the Value.
	 */
	public void insertRowToTable(String rowkey, String columnFamily, Map<String, String> content) {
		
		for(String qualifier : content.keySet()) {
			String value = content.get(qualifier);
			Put putData = createPut(rowkey, columnFamily, qualifier, value);
			puts.add(putData);
		}
		
	}
	
	/*
	 * Transmit data of each row to remote hbase
	 * @param table is the htable object
	 */
	public void updateHTable(HTable table) {
		table.setAutoFlushTo(false);;
		try {
			table.put(puts);
		} catch (RetriesExhaustedWithDetailsException e) {
			log.error("EconomicDataImport class, table.put \n" + e.toString());
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			log.error("EconomicDataImport class, table.put \n" + e.toString());
			e.printStackTrace();
		}
	}

	
	/**
	 * Read the entire .csv file, import 'date' and 'value' into the HashMap
	 * @param filename, the .csv file
	 * @return the HashMap involving dates and values
	 * @throws IOException
	 */
	public Map<String, String> getData(String filename)  {
		Map<String, String> output = new HashMap<String, String>();
		try {
			CSVReader reader = new CSVReader(new FileReader(filename));
			String[] line;
			
			while ((line = reader.readNext()) != null) {
				if (line.length == 2) {
					output.put(line[0], line[1]);
					
					//[Lucas], find out the started date and the ended date
					initializeStartAndEndDates(line[0]);
				}
			}
			reader.close();
			//System.out.println("TEST:\n first: " + sdf.format(get_StartDate()) + "\n second: " + sdf.format(get_StopDate()));
		} catch (FileNotFoundException e) {
			log.error("EconomicDataImport class\n" + e.toString());
			e.printStackTrace();
		} catch (IOException e) {
			log.error("EconomicDataImport class\n" + e.toString());
			e.printStackTrace();
		}
		
		return output;
	}

	protected abstract Date calculateEndDate();
	protected abstract void doDataAnalyze(Map<String, String> data, HTable table);
	
}
