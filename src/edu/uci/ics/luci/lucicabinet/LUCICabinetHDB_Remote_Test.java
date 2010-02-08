package edu.uci.ics.luci.lucicabinet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.luci.lucicabinet.library.IteratorWorkerCountEntries;


public class LUCICabinetHDB_Remote_Test {
	
	LUCICabinetHDB<Integer,String> hdbl = null;
	LUCI_Butler<Integer,String> butler = null;
	LUCICabinetHDB_Remote<Integer,String> hdb_remote = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		BasicConfigurator.configure();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}
	
	private static class TestAccessControl extends AccessControl{
		@Override
		public boolean allowSource(String source) {
			if(source.equals("/127.0.0.1")){
				return true;
			}
			else if(source.equals("127.0.0.1")){
				return true;
			}
			else{
				return false;
			}
		}
	};

	@Before
	public void setUp() throws Exception {
		
		try{
			hdbl = new LUCICabinetHDB<Integer,String>("eraseme.tch");
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		butler = new LUCI_Butler<Integer,String>(hdbl,8181,new TestAccessControl());
		butler.initialize();
		
		try{
			hdb_remote = new LUCICabinetHDB_Remote<Integer,String>("localhost",8181);
		} catch (UnknownHostException e) {
			fail("This shouldn't throw an exception"+e);
		} catch (IOException e) {
			fail("This shouldn't throw an exception"+e);
		} catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
	}

	@After
	public void tearDown() throws Exception {
		try{
			if(hdb_remote != null){
				hdb_remote.close();
				hdb_remote = null;
			}
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		try{
			if(butler != null){
				butler.shutdown();
				butler = null;
			}
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		try{
			if(hdbl != null){
				hdbl.close();
				hdbl = null;
			}
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
	}


	@Test
	public void testPutGetOut() {
		for(Integer key=0; key< 1000; key++){
			String value = "foo"+key;
			hdb_remote.put(key,value);
		}
			
		for(Integer key=0; key< 1000; key++){
			String x = (String) hdb_remote.get(key);
			assertEquals("foo"+key,x);
		}
		
		assertEquals(1000,hdb_remote.size());
		assertEquals(1000,hdb_remote.sizeLong());
		
		for(Integer key=0; key< 1000; key++){
			hdb_remote.remove(key);
		}
			
		for(Integer key=0; key< 1000; key++){
			String x = hdb_remote.get(key);
			assertTrue(x == null);
		}
	}


	@SuppressWarnings("unchecked")
	@Test
	public void testIterate() {
		
		for(Integer key=0; key< 1000; key++){
			String value = "foo"+key;
			hdb_remote.put(key, value);
		}
		assertEquals(1000,hdb_remote.size());
			
		IteratorWorkerCountEntries<Integer, String> iw = null;
		try{
			iw = (IteratorWorkerCountEntries<Integer, String>) hdb_remote.iterate((Class<? extends IteratorWorker<Integer, String>>) IteratorWorkerCountEntries.class,null);
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
						hdb_remote.put(key, value);
						hdb_remote.get(key);
					}
					try {
						hdb_remote.iterateASync((Class<? extends IteratorWorker<Integer, String>>) IteratorWorkerCountEntries.class,null);
					} catch (InstantiationException e) {
						fail("This shouldn't throw an exception"+e);
					} catch (IllegalAccessException e) {
						fail("This shouldn't throw an exception"+e);
					}
				}
					
			}
		};
		
		Runnable local = new Runnable(){
			@SuppressWarnings("unchecked")
			public void run() {
				for(int j=0; j< 10; j++){
					for(Integer key=0; key< number; key++){
						String value = "foo"+key;
						hdb_remote.put(key, value);
						hdb_remote.get(key);
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
		for(int i =0; i< threadnumber; i+=2){
			t[i] = new Thread(remote);
			t[i+1] = new Thread(local);
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
		System.out.println(""+(threadnumber*10*number)+" local puts, "+(threadnumber*10*number)+" remote puts, "+(threadnumber*10*number)+" local gets, and "+(threadnumber*10*number)+" remote gets in "+duration+" milliseconds");
		System.out.println(""+(duration/(2*((threadnumber*10*number)+(threadnumber*10*number))))+" milliseconds per operation");
		testIterate();
	}
}