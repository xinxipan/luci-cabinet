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

public class LUCICabinetBDB_Test {

	LUCICabinetBDB<Integer,String> bdbl = null;

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
			bdbl = new LUCICabinetBDB<Integer,String>("eraseme.tcb");
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		bdbl.iterate((Class<? extends IteratorWorker<Integer, String>>) IteratorWorkerRemoveAll.class,new IteratorWorkerConfig());
	}

	@SuppressWarnings("unchecked")
	@After
	public void tearDown() throws Exception {
		if(bdbl != null){
			bdbl.iterate((Class<? extends IteratorWorker<Integer, String>>) IteratorWorkerRemoveAll.class,new IteratorWorkerConfig());
		
			try{
				bdbl.close();
			}
			catch(RuntimeException e){
				fail("This shouldn't throw an exception"+e);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testOpenClose() {
		LUCICabinetBDB<String,String> bdb = null;
		try{
			bdb = new LUCICabinetBDB<String,String>("eraseme2.tcb");
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		try {
			bdb.iterate((Class<? extends IteratorWorker<String, String>>) IteratorWorkerRemoveAll.class,new IteratorWorkerConfig());
		} catch (InstantiationException e) {
			fail("This shouldn't throw an exception"+e);
		} catch (IllegalAccessException e) {
			fail("This shouldn't throw an exception"+e);
		}
		
		assertEquals(0,bdb.size());
		assertEquals(0,bdb.sizeLong());
		
		try{
			bdb.remove("foo");
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		try{
			bdb.put("foo","bar");
			bdb.put("foo","baz");
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		assertEquals(1,bdb.size());
		assertEquals(1,bdb.sizeLong());
		
		try {
			bdb.iterate((Class<? extends IteratorWorker<String, String>>) IteratorWorkerRemoveAll.class,new IteratorWorkerConfig());
		} catch (InstantiationException e) {
			fail("This shouldn't throw an exception"+e);
		} catch (IllegalAccessException e) {
			fail("This shouldn't throw an exception"+e);
		}
		
		assertEquals(0,bdb.size());
		assertEquals(0,bdb.sizeLong());
		
		try{
			bdb.close();
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
	}


	@Test
	public void testPutGetOut() {
		for(Integer key=0; key< 1000; key++){
			String value = "foo"+key;
			bdbl.put(key,value);
		}
		
		assertEquals(1000,bdbl.size());
		assertEquals(1000,bdbl.sizeLong());
		
		for(Integer key=0; key< 1000; key++){
			String x = (String) bdbl.get(key);
			assertEquals("foo"+key,x);
		}
		
		assertEquals(1000,bdbl.size());
		assertEquals(1000,bdbl.sizeLong());
		
		for(Integer key=0; key< 1000; key++){
			bdbl.remove(key);
		}
		
		assertEquals(0,bdbl.size());
		assertEquals(0,bdbl.sizeLong());
		
		for(Integer key=0; key< 1000; key++){
			String x = (String) bdbl.get(key);
			assertTrue(x == null);
		}
	}


	@SuppressWarnings("unchecked")
	@Test
	public void testIterate() {
		
		for(Integer key=0; key< 1000; key++){
			String value = "foo"+key;
			bdbl.put(key,value);
		}
		assertEquals(1000,bdbl.size());
		
		IteratorWorkerCountEntries<Integer,String> iw = null;
		try{
			iw = (IteratorWorkerCountEntries<Integer, String>) bdbl.iterate((Class<? extends IteratorWorker<Integer, String>>) IteratorWorkerCountEntries.class,new IteratorWorkerConfig());
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
						bdbl.put(key, value);
						bdbl.get(key);
					}
					try {
						bdbl.iterate((Class<? extends IteratorWorker<Integer, String>>) IteratorWorkerCountEntries.class,null);
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
