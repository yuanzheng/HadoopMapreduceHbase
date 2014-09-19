package EconomicDataImport.DataImport;


import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;


import junit.framework.TestCase;


@RunWith(MockitoJUnitRunner.class)
public class CPIImportTest extends TestCase {
	@Mock
	private CPIImport mockEcon;
	@Mock
	private HTable htable;
	@Mock
	private Configuration conf;
	private final String file = "src/test/resources/CPI_2014-07-01.csv"; // You may change it to the one 
	private SimpleDateFormat sdf;
	private Map<String, String> data;
	
	@Before
	public void setUp() throws Exception {
		sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		mockEcon = Mockito.spy(new CPIImport(htable));
		
		data = mockEcon.getData(file);
		assertTrue(data.size() == 812);
	}
	
	/*
	 * Test getmomPctStr(), ensure the calculation is correct.
	 */
	@Test
	public void testGetmomPctStr() {
		
		assertTrue(data.size() == 812);
		 
		String momPctStr = "0.20";
		String val = "235.640";
		Date monCal;
		try {
			monCal = sdf.parse("2014-02-01");
			Calendar prevMonthCal = Calendar.getInstance();
			prevMonthCal.setTime(monCal);
			
			Method getmomPctStr = CPIImport.class.getDeclaredMethod("getmomPctStr", new Class<?>[]{Calendar.class, Map.class, String.class});
			getmomPctStr.setAccessible(true);
			
			String return_value = (String) getmomPctStr.invoke(mockEcon, prevMonthCal, data, val);
			assertTrue(return_value.equals(momPctStr));
			//System.out.println("Testing return_value: " + return_value);
			
		} catch (ReflectiveOperationException e) {
			e.printStackTrace();
		} catch (RuntimeException e) {
			e.printStackTrace();
		} catch (ParseException e1) {
			e1.printStackTrace();
		}	
	}

	/*
	 * Test getyoyPctStr(), ensure the calculation is correct.
	 */
	@Test
	public void testGetyoyPctStr() {
		String yoyPctStr = "1.54";
		String val = "235.640";
		
		Date yearCal;
		try {
			yearCal = sdf.parse("2013-03-01");
			Calendar prevYearCal = Calendar.getInstance();
			prevYearCal.setTime(yearCal);
			
			Method getyoyPctStr = CPIImport.class.getDeclaredMethod("getyoyPctStr", new Class<?>[]{Calendar.class, Map.class, String.class});
			getyoyPctStr.setAccessible(true);
			
			String return_value = (String) getyoyPctStr.invoke(mockEcon, prevYearCal, data, val);
			assertTrue(return_value.equals(yoyPctStr));
		
		} catch (ReflectiveOperationException e) {
			e.printStackTrace();
		} catch (RuntimeException e) {
			e.printStackTrace();
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
	}
	
	/*
	 * Test doDataAnalyze()
	 * Base the CPI_2014-07-01.csv, Puts list should have 99104 single put
	 * because each rowkey has 4 puts. Totally 24776 rowkeys.
	 */
	@Test
	public void testDoDataAnalyze() {
		
		mockEcon.doDataAnalyze(data, htable);

		Field puts_field;
		try {
			puts_field = EconomicDataImport.class.getDeclaredField("puts");
			puts_field.setAccessible(true);
			List<Put> puts = (ArrayList<Put>) puts_field.get(mockEcon);
			
			assertTrue(puts.size() == 99104);
			
			String rowkey = "2014-09-20";
			String value = "237.909";
			//System.out.println("line: " + Bytes.toString(puts.get(98939).getRow()));
			assertTrue(rowkey.equals(Bytes.toString(puts.get(98939).getRow())));
			assertTrue(puts.get(98937).has(Bytes.toBytes("data"), Bytes.toBytes("value"), Bytes.toBytes(value)));

		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void testImportCPI() {

		mockEcon.importCPI(file);
	
		DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");
		
		Date endDate = mockEcon.calculateEndDate();
		// Warning: Based on CPI_2014-07-01.csv, the last date is "2014-11-01"
		assertTrue(dateformat.format(endDate).equals("2014-11-01"));
		
	}
	
}
