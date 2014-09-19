/**
 * @author Lucas Song <ysong@insidesales.com>
 */
package EconomicDataImport.DataImport;

import static org.junit.Assert.*;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class GDPImportTest {

	@Mock
	private GDPImport mockEcon;
	@Mock
	private HTable htable;
	@Mock
	private Configuration conf;
	private final String file = "src/test/resources/GDP_2014-04-01.csv"; // You may change it to the one 
	private SimpleDateFormat sdf;
	private Map<String, String> data;
	
	
	@Before
	public void setUp() throws Exception {
		sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		mockEcon = Mockito.spy(new GDPImport(htable));
		
		data = mockEcon.getData(file);
		//System.out.println("Check GDP: " + data.size());
		assertTrue(data.size() == 271);
	}

	/*
	 * Test getqoqPctStr(), ensure the calculation is correct.
	 */
	@Test
	public void testGetqoqPctStr() {
				 
		String qoqPctStr = "1.52";
		String val = "16872.3";
		Date monCal;
		try {
			monCal = sdf.parse("2013-04-01");
			Calendar prevMonthCal = Calendar.getInstance();
			prevMonthCal.setTime(monCal);
			
			Method getqoqPctStr = GDPImport.class.getDeclaredMethod("getqoqPctStr", new Class<?>[]{Calendar.class, Map.class, String.class});
			getqoqPctStr.setAccessible(true);
			
			String return_value = (String) getqoqPctStr.invoke(mockEcon, prevMonthCal, data, val);
			assertTrue(return_value.equals(qoqPctStr));
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
		String yoyPctStr = "3.71";
		String val = "16872.3";
		
		Date yearCal;
		try {
			yearCal = sdf.parse("2012-07-01");
			Calendar prevYearCal = Calendar.getInstance();
			prevYearCal.setTime(yearCal);
			
			Method getyoyPctStr = GDPImport.class.getDeclaredMethod("getyoyPctStr", new Class<?>[]{Calendar.class, Map.class, String.class});
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
	

	/**
	 * Test method for {doDataAnalyze(java.util.Map, org.apache.hadoop.hbase.client.HTable)}.
	 * Puts list should have 99104 single put because each rowkey has 4 puts. Totally 24776 rowkeys.
	 * Check the insert data in order to judge the correctness
	 */
	@Test
	public void testDoDataAnalyze() {
		mockEcon.doDataAnalyze(data, htable);
		
		Field puts_field;
		try {
			puts_field = EconomicDataImport.class.getDeclaredField("puts");
			puts_field.setAccessible(true);
			List<Put> puts = (ArrayList<Put>) puts_field.get(mockEcon);
			
			//System.out.println("Check GDP: " + puts.size());
			assertTrue(puts.size() == 99104);
			
			String rowkey = "2014-03-01";
			String value = "17044.0";
			
			//System.out.println("line: " + Bytes.toString(puts.get(98127).getRow()));
			assertTrue(rowkey.equals(Bytes.toString(puts.get(98127).getRow())));
			assertTrue(puts.get(98125).has(Bytes.toBytes("data"), Bytes.toBytes("value"), Bytes.toBytes(value)));
			
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

	/**
	 * Test method for {importGDP(java.lang.String)}.
	 */
	@Test
	public void testImportGDP() {
		
		mockEcon.importGDP(file);
		DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");
		
		Date endDate = mockEcon.calculateEndDate();
		// Warning: Based on data from GDP_2014-04-01.csv, the last date is "2014-11-01"
		assertTrue(dateformat.format(endDate).equals("2014-11-01"));
	}

}
