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

public class CPIImport extends EconomicDataImport {

	private final int addNumberofMonth = 4;
	private Configuration conf;
	private HTable cpiTable;
	private DateFormat dateformat; 
	static Logger log = Logger.getLogger(CPIImport.class.getName());

	/* Testing purpose */
	public CPIImport(HTable htable) {
		conf = null;
		cpiTable = htable;
		dateformat = new SimpleDateFormat("yyyy-MM-dd");
	}

	public CPIImport(String htableName) {
		
		conf = getConfiguration();
		
		try {
			cpiTable = new HTable(conf, htableName);
		} catch (IOException e) {
			log.error("CPIImport class, Htable connection is failed!\n" + e.toString());
			e.printStackTrace();
		}
		
		dateformat = new SimpleDateFormat("yyyy-MM-dd");
	}
	
	/**
	 * [Lucas] 
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
	
	private String getmomPctStr(Calendar prevMonthCal, Map<String, String> data, String val) {
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
	
	/*
	 * Modified by Lucas
	 */
	protected void doDataAnalyze(Map<String, String> data, HTable table) {
		
		Calendar cal = Calendar.getInstance();
		setDate(cal, getStartDate());
		Calendar stopDate = Calendar.getInstance();	
		// We only need to change the following line to update cpi data
		// It has 3 month lag for cpi data
		setDate(stopDate, calculateEndDate());
		
		Calendar prevMonthCal = Calendar.getInstance();
		setDate(prevMonthCal, getStartDate());
		Calendar prevYearCal = Calendar.getInstance();
		setDate(prevYearCal, getStartDate());

		String startkey = dateformat.format(cal.getTime());
		String val = data.get(startkey);
		String momPctStr = "";
		String yoyPctStr = "";
		int monthCount = 0;
		while (cal.before(stopDate)) {
			String rowkey = dateformat.format(cal.getTime());
			if (data.containsKey(rowkey)) {
				val = data.get(rowkey);
				if (monthCount > 1) {
					prevMonthCal.add(Calendar.MONTH, 1);
				}
				if ((monthCount > 12)) {
					prevYearCal.add(Calendar.MONTH, 1);
				}
				
				momPctStr = getmomPctStr(prevMonthCal, data, val);
				yoyPctStr = getyoyPctStr(prevYearCal, data, val);
			
				monthCount++;
			}
			
			String columnFamily = "data";
			Map<String, String> content = new HashMap<String, String>(); 
			content.put("date", rowkey);
			content.put("value", val);
			content.put("mom_pct", momPctStr);
			content.put("yoy_pct", yoyPctStr);
			insertRowToTable(rowkey, columnFamily, content);
			   			
			cal.add(Calendar.DATE, 1);
		}
		
		updateHTable(table);
	}

	/*
	 * [Lucas] Import data from the indicated path into the given
	 * 		   htable
	 * @param path: the cpi file is located there
	 * @param htableName: the htable name
	 */
	public void importCPI(String path) {
		//EconomicDataImport edi_cpi = new CPIImport();
		Map<String, String> data = getData(path);
		log.info("We got data from CPI file!!");
		
		try {
			doDataAnalyze(data, cpiTable);
			log.info("Data is transmitted to htable.");
			cpiTable.close();
			log.info("Finishing updating cpi data.");
		} catch (IOException e) {
			log.error("CPIImport class, htable close failed!\n" + e.toString());
			e.printStackTrace();
		}
 
	}

}
