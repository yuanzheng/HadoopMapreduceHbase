package EconomicDataImport.DataImport;

/*
 * Created by Lucas Song <ysong@insidesales.com>
 */
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.log4j.Logger;

public class GDPImport extends EconomicDataImport {

	private final int addNumberofMonth = 7;
	private Configuration conf;
	private HTable gdpTable;
	private DateFormat dateformat; 
	static Logger log = Logger.getLogger(GDPImport.class.getName());
	
	/* Testing purpose */
	public GDPImport(HTable htable) {
		conf = null;
		gdpTable = htable;
		dateformat = new SimpleDateFormat("yyyy-MM-dd");
	}
	
	public GDPImport(String htableName) {
		
		conf = getConfiguration();
		
		try {
			gdpTable = new HTable(conf, htableName);
		} catch (IOException e) {
			log.error("GDPImport class, Htable connection is failed!\n" + e.toString());
			e.printStackTrace();
		}
		
		dateformat = new SimpleDateFormat("yyyy-MM-dd");
	}
	
	/**
	 * Based on the requirement that has 3 month lag for cpi data.
	 * We change the last month in the CPI file to 4 month later
	 * @return the ended date
	 */
	protected Date calculateEndDate() {
		Calendar cal = Calendar.getInstance();
		
		Date date = getStopDate();
		cal.setTime(date);
		cal.add(Calendar.MONTH, addNumberofMonth);
		
		return cal.getTime();
	}
	
	private void setDate(Calendar cal, Date date) {
		
		cal.setTime(date); 
	}
	
	
	private String getqoqPctStr(Calendar prevMonthCal, Map<String, String> data, String val) {
		String prevMonthKey = dateformat.format(prevMonthCal.getTime());

		double prevMonthVal = Double.parseDouble(data.get(prevMonthKey));
		double currentVal = Double.parseDouble(val);
		double mompct = 100.0 * (currentVal - prevMonthVal) / prevMonthVal;
		
		return String.format("%.2f", mompct);
	}

	private String getyoyPctStr(Calendar prevYearCal, Map<String, String> data, String val) {
		String prevYearKey = dateformat.format(prevYearCal.getTime());
		double prevYearVal = Double.parseDouble(data.get(prevYearKey));
		double currentVal = Double.parseDouble(val);
		double yoypct = 100.0 * (currentVal - prevYearVal) / prevYearVal;

		return String.format("%.2f", yoypct);
		
	}


	/**
	 * Modified by Lucas
	 * Import the content of GDP file into HBase
	 */
	protected void doDataAnalyze(Map<String, String> data, HTable table) {

		Calendar cal = Calendar.getInstance();
		setDate(cal, getStartDate());

		Calendar stopDate = Calendar.getInstance();
		// We only need to change the following line to update gdp data
		// It has 6 months lag for gdp_data
		setDate(stopDate, calculateEndDate());

		Calendar prevMonthCal = Calendar.getInstance();
		setDate(prevMonthCal, getStartDate());
		
		Calendar prevYearCal = Calendar.getInstance();
		setDate(prevYearCal, getStartDate());
		
		String startkey = dateformat.format(cal.getTime());
		String val = data.get(startkey);
		String qoqPctStr = "";
		String yoyPctStr = "";
		int monthCount = 0;
		while (cal.before(stopDate)) {
			String rowkey = dateformat.format(cal.getTime());
			if (data.containsKey(rowkey)) {
				val = data.get(rowkey);
				if (monthCount > 3) {
					prevMonthCal.add(Calendar.MONTH, 3);
				}
				if ((monthCount > 12)) {
					prevYearCal.add(Calendar.MONTH, 3);
				}
				
				qoqPctStr = getqoqPctStr(prevMonthCal, data, val);
				yoyPctStr = getyoyPctStr(prevYearCal, data, val);
				
				monthCount += 3;
			}
			
			String columnFamily = "data";
			Map<String, String> content = new HashMap<String, String>(); 
			content.put("date", rowkey);
			content.put("value", val);
			content.put("qoq_pct", qoqPctStr);
			content.put("yoy_pct", yoyPctStr);
			insertRowToTable(rowkey, columnFamily, content);
			
			cal.add(Calendar.DATE, 1);
			
		}
		updateHTable(table);	
	}
	
	/*
	 * [Lucas] Import data from the indicated path into the given
	 * 		   htable
	 * @param path: the gdp file is located there
	 * @param htableName: the htable name
	 */
	public void importGDP(String path) {
		//EconomicDataImport edi_gdp = new GDPImport();
		Map<String, String> data = getData(path);
		log.info("We got data from GDP file!!");
		
		try {
			doDataAnalyze(data, gdpTable);
			log.info("Data is transmitted to htable.");
			gdpTable.close();
			log.info("Finishing updating gdp data.");	
		} catch (IOException e) {
			log.error("GDPImport class, htable close failed!\n" + e.toString());
			e.printStackTrace();
		}
	}
	
}