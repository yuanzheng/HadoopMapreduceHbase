package EconomicDataImport.DataImport;

/*
 * Created by Lucas <ysong@insidesales.com> 
 */
import static org.junit.Assert.*;

import java.io.InterruptedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;


/*
 * [Lucas] Warning! Before starting this test, first, you have to modify the file name,
 * the start date, and the end date.
 */
@RunWith(MockitoJUnitRunner.class)
public class EconomicDataImportTest {
	@Mock
	private EconomicDataImport mockEcon;
	@Mock
	private HTable htable;
	@Captor
	private ArgumentCaptor<Put> putCaptor;
	private final String file = "src/test/resources/CPI_2014-07-01.csv"; // File is for testing
	private final int fileSize = 812;
	private DateFormat dateformat; 
	/*
	 * Based on the latest CPI, we found the start date and end date below. 
	 */
	private final String startDate = "1947-01-01";    // Give the date, manually
	private final String endDate = "2014-07-01";    // Give the date, manually
	
	
	/*
	 * EconomicDataImport is an abstract class, in order to test methods
	 * We must mock it by implementing an anonymous concrete class and spy it.
	 */
	@Before
	public void setUp() throws Exception {
		
		mockEcon = Mockito.spy(new EconomicDataImport() {
			@Override
			protected Date calculateEndDate() {
				return null;  // for the testing purpose
			}
			@Override
			protected void doDataAnalyze(Map<String, String> data, HTable table) {
				System.out.println("Hello Tester");
			}
		});
		
		dateformat = new SimpleDateFormat("yyyy-MM-dd");
	}
	
	/*
	 * Test private variables, after object initialization
	 */
	@Test
	public void testPrivateFields() {
		
		try {
			Field StartEnd_field = EconomicDataImport.class.getDeclaredField("StartEnd");
			StartEnd_field.setAccessible(true);
			assertNotNull(StartEnd_field.get(mockEcon));
			
			Field sdf_field = EconomicDataImport.class.getDeclaredField("sdf");
			sdf_field.setAccessible(true);
			assertNotNull(sdf_field.get(mockEcon));
			
			Field conf_field = EconomicDataImport.class.getDeclaredField("conf");
			conf_field.setAccessible(true);
			//assertTrue(conf_field.get(mockEcon) instanceof Configuration);
			assertNotNull(conf_field.get(mockEcon));
			
			Field puts_field = EconomicDataImport.class.getDeclaredField("puts");
			puts_field.setAccessible(true);
			assertNotNull(puts_field.get(mockEcon));
			
		} catch (ReflectiveOperationException e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * Test getData() 
	 * Read file from local drive and build Map  
	 */
	@Test
	public void testGetData() {
	
		Map<String, String> output = mockEcon.getData(file);
		assertTrue(output.size() == fileSize);
				
	}
	
	/*
	 * Test insertRowToTable(...), and createPut()
	 */
	@Test
	public void testInsertRowToTable() {
		String rowkey = "1947-01-01";
		String value = "100";
	
		try {
			Field puts_field = EconomicDataImport.class.getDeclaredField("puts");
			puts_field.setAccessible(true);
			List<Put> puts = (ArrayList<Put>) puts_field.get(mockEcon);
			
			assertTrue(puts.isEmpty());
			buildPutforHtable(rowkey, value);
			assertTrue(rowkey.equals(Bytes.toString(puts.get(0).getRow())));
			assertTrue(puts.get(0).has(Bytes.toBytes("data"), Bytes.toBytes("value"), Bytes.toBytes(value)));

			assertTrue(puts.size()==1);
			
		} catch (ReflectiveOperationException e) {
			e.printStackTrace();
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
		
	}
	
	
	// create put content
	private void buildPutforHtable(String rowkey, String value) {

		String columnFamily = "data";
		Map<String, String> content = new HashMap<String, String>();

		content.put("value", value);
		
		mockEcon.insertRowToTable(rowkey, columnFamily, content);		
	}

	
	/*
	 * Test updateHTable(HTable table), and createPut()
	 * Ensure all put data is correct in HTable. And table.setAutoFlashTo() is called!
	 */
	@Test
	public void testUpdateHTable() {
		Field puts_field;
		
		try {
			puts_field = EconomicDataImport.class.getDeclaredField("puts");
			puts_field.setAccessible(true);
			List<Put> puts = (ArrayList<Put>) puts_field.get(mockEcon);
			assertTrue(puts.isEmpty());
			
			String rowkey = "1947-01-01";
			String value = "100";
			buildPutforHtable(rowkey, value);
			
			mockEcon.updateHTable(htable); // call updateHTable()
	
			// table.setAutoFlashTo() is called
			Mockito.verify(htable, Mockito.times(1)).setAutoFlushTo(false);
		
			// capture the puts list variable
			Mockito.verify(htable).put((List<Put>) putCaptor.capture());
			List<?> trace_puts = putCaptor.getAllValues();
			
			assertFalse(trace_puts.isEmpty());
			//System.out.println("See " + trace_puts.toString());
			
			List<Put> test_puts = (ArrayList<Put>) trace_puts.get(0);
			
			// check the data inserted into htable has the right rowkey and qualifier, and value
			assertTrue(rowkey.equals(Bytes.toString(test_puts.get(0).getRow())));
			assertTrue(test_puts.get(0).has(Bytes.toBytes("data"), Bytes.toBytes("value"), Bytes.toBytes(value)));
			//System.out.println("See 1: " + test.get(0).toString());
			//System.out.println("See 2: " + test.get(0).get(Bytes.toBytes("data"), Bytes.toBytes("value")).toString());
			//System.out.println("See again: " + test.get(0).get(Bytes.toBytes("date"), Bytes.toBytes("value")).  );
		} catch (ReflectiveOperationException e) {
			e.printStackTrace();
		} catch (RuntimeException e) {
			e.printStackTrace();
		} catch (RetriesExhaustedWithDetailsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/*
	 * Configuation conf can be only initialized once. Check if both 'first' and 
	 * 'second' indicate to the same object.
	 */
	@Test
	public void testGetConfiguration() {
		//fail("Not yet implemented");
		Configuration first = mockEcon.getConfiguration();
		Configuration second = mockEcon.getConfiguration();
		assertNotNull(first);
		assertNotNull(second);
		assertSame(first, second);
	}

	/*
	 * Test the private method
	 */
	@Test
	public void testinitializeStartAndEndDates() {
		String date1 = "2014-02-01";
		String date2 = "2014-01-05";
		Field StartEnd_field;
		try {
			StartEnd_field = EconomicDataImport.class.getDeclaredField("StartEnd");
			StartEnd_field.setAccessible(true);
			
			List<Date> StartEnd =  (ArrayList<Date>) StartEnd_field.get(mockEcon);
			assertTrue(StartEnd.isEmpty());
			
			Method privateMethod = EconomicDataImport.class.getDeclaredMethod("initializeStartAndEndDates", new Class<?>[]{String.class});
			privateMethod.setAccessible(true);
			
			privateMethod.invoke(mockEcon, date1);
			assertFalse(StartEnd.isEmpty());
			privateMethod.invoke(mockEcon, date2);
			assertTrue(StartEnd.get(0).before(StartEnd.get(1)));
			
		} catch (ReflectiveOperationException e) {
			e.printStackTrace();
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
		
	}
	
	/*
	 * Test getStartDate()
	 */
	@Test
	public void testGetStartDate() {
		mockEcon.getData(file);
		String date = dateformat.format(mockEcon.getStartDate());
		
		assertTrue(date.compareTo(startDate)==0);
	}

	/*
	 * Test getStopDate()
	 */
	@Test
	public void testGetStopDate() {
		mockEcon.getData(file);
		String date = dateformat.format(mockEcon.getStopDate());
		assertTrue(date.compareTo(endDate)==0);
	}

	
}
