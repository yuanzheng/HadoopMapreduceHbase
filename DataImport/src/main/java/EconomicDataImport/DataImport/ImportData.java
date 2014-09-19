package EconomicDataImport.DataImport;

import org.apache.log4j.Logger;

/**
 * Created by Lucas Song <ysong@insidesales.com>
 */

public class ImportData {

	static Logger log = Logger.getLogger(ImportData.class.getName());
	/**
	 * [Lucas]
	 * @param args[0] is the htable name
	 * @param args[1] is the path of the imported file
	 */
	public static void main(String[] args) {
		log.warn("java -jar ImportData.jar " + args[0] + ", " + args[1]);
		String htableName = args[0];
		String csvFile = args[1];
		
		if (htableName.equals("cpi_data_test") || 
				htableName.equals("cpi_data")) {
	
			CPIImport cpi = new CPIImport(htableName);
			cpi.importCPI(csvFile);
			
		}
		else if (htableName.equals("gdp_data_test") || 
				htableName.equals("gdp_data")) {
			
			GDPImport gdp = new GDPImport(htableName);
			gdp.importGDP(csvFile);
			
		}
		else {
			log.error("Invalid htable name: " + htableName);
            throw new IllegalArgumentException("Invalid file path: " + htableName);
		}
	}

}
