package edu.uci.ics.luci.lucicabinet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.luci.lucicabinet.library.IteratorWorkerCountEntries;
import edu.uci.ics.luci.lucicabinet.library.IteratorWorkerRemoveAll;

public class LUCICabinetHDB_Test {

	LUCICabinetHDB<Integer, String> hdbl = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		BasicConfigurator.configure();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}
	
	@Before
	@SuppressWarnings("unchecked")
	public void setUp() throws Exception {
		try{
			hdbl = new LUCICabinetHDB<Integer,String>("eraseme.tch");
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		hdbl.iterate((Class<? extends IteratorWorker<Integer, String>>) IteratorWorkerRemoveAll.class,new IteratorWorkerConfig());
	}

	@SuppressWarnings("unchecked")
	@After
	public void tearDown() throws Exception {
		if(hdbl != null){
			hdbl.iterate((Class<? extends IteratorWorker<Integer, String>>) IteratorWorkerRemoveAll.class,new IteratorWorkerConfig());

			try{
				hdbl.close();
			}
			catch(RuntimeException e){
				fail("This shouldn't throw an exception"+e);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testOpenClose() {
		LUCICabinetHDB<String, String> hdb = null;
		try{
			hdb = new LUCICabinetHDB<String,String>("eraseme2.tch");
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		try {
			hdb.iterate((Class<? extends IteratorWorker<String, String>>) IteratorWorkerRemoveAll.class,new IteratorWorkerConfig());
		} catch (InstantiationException e) {
			fail("This shouldn't throw an exception"+e);
		} catch (IllegalAccessException e) {
			fail("This shouldn't throw an exception"+e);
		}
		
		assertEquals(0,hdb.size());
		assertEquals(0,hdb.sizeLong());
		
		try{
			hdb.remove("foo");
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		try{
			hdb.put("foo","bar");
			hdb.put("foo","baz");
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		assertEquals(1,hdb.size());
		assertEquals(1,hdb.sizeLong());
		
		try{
			hdb.iterate((Class<? extends IteratorWorker<String, String>>) IteratorWorkerRemoveAll.class,new IteratorWorkerConfig());
		} catch (InstantiationException e) {
			fail("This shouldn't throw an exception"+e);
		} catch (IllegalAccessException e) {
			fail("This shouldn't throw an exception"+e);
		}
		
		assertEquals(0,hdb.size());
		assertEquals(0,hdb.sizeLong());
		
		try{
			hdb.close();
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
	}


	@Test
	public void testPutGetOut() {
		for(Integer key=0; key< 1000; key++){
			String value = "foo"+key;
			hdbl.put(key,value);
		}
		
		assertEquals(1000,hdbl.size());
		assertEquals(1000,hdbl.sizeLong());
		
		for(Integer key=0; key< 1000; key++){
			String x = (String) hdbl.get(key);
			assertEquals("foo"+key,x);
		}
		
		assertEquals(1000,hdbl.size());
		assertEquals(1000,hdbl.sizeLong());
		
		for(Integer key=0; key< 1000; key++){
			hdbl.remove(key);
		}
		
		assertEquals(0,hdbl.size());
		assertEquals(0,hdbl.sizeLong());
		
		for(Integer key=0; key< 1000; key++){
			String x = (String) hdbl.get(key);
			assertTrue(x == null);
		}
	}


	@SuppressWarnings("unchecked")
	@Test
	public void testIterate() {
		
		for(Integer key=0; key< 1000; key++){
			String value = "foo"+key;
			hdbl.put(key,value);
		}
		assertEquals(1000,hdbl.size());
		
		IteratorWorkerCountEntries<Integer,String> iw = null;
		try{
			iw = (IteratorWorkerCountEntries<Integer, String>) hdbl.iterate((Class<? extends IteratorWorker<Integer, String>>) IteratorWorkerCountEntries.class,new IteratorWorkerConfig());
		} catch (InstantiationException e) {
			fail("This shouldn't throw an exception"+e);
		} catch (IllegalAccessException e) {
			fail("This shouldn't throw an exception"+e);
		} catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		assertEquals(1000,iw.count);
		assertTrue(iw.ranInit);
		assertTrue(iw.ranShutdown);
	}
	
	@Test
	public void testForDeadlock() {
		final int number = 75;
		
		/*This finishes if there is no deadlock, this doesn't guarantee no deadlocks can happen though */
		Runnable remote = new Runnable(){
			@SuppressWarnings("unchecked")
			public void run() {
				for(int j=0; j< 10; j++){
					for(Integer key=0; key< number; key++){
						String value = "foo"+key;
						hdbl.put(key, value);
						hdbl.get(key);
					}
					try {
						hdbl.iterate((Class<? extends IteratorWorker<Integer, String>>) IteratorWorkerCountEntries.class,null);
					} catch (InstantiationException e) {
						fail("This shouldn't throw an exception"+e);
					} catch (IllegalAccessException e) {
						fail("This shouldn't throw an exception"+e);
					}
				}
				
			}
		};
		
		final int threadnumber = 10;
		Thread[] t = new Thread[threadnumber];
		for(int i =0; i< threadnumber; i++){
			t[i] = new Thread(remote);
		}
		
		long start = System.currentTimeMillis();
		for(int i =0; i< threadnumber; i++){
			t[i].start();
		}
		for(int i =0; i< threadnumber; i++){
			try {
				t[i].join();
			} catch (InterruptedException e) {
				fail("This shouldn't be interrupted"+e);
			}
		}
		
		double duration = System.currentTimeMillis()-start;
		System.out.println(""+(threadnumber*10*number)+" puts and "+(threadnumber*10*number)+" gets in "+duration+" milliseconds");
		System.out.println(""+(duration/((threadnumber*10*number)+(threadnumber*10*number)))+" milliseconds per operation");
		testIterate();
	}
}
