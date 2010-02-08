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


public class LUCICabinetBDB_Remote_Test {
	
	LUCICabinetBDB<Integer,String> bdbl = null;
	LUCI_Butler<Integer,String> butler = null;
	LUCICabinetBDB_Remote<Integer,String> bdbl_remote = null;

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
			bdbl = new LUCICabinetBDB<Integer,String>("eraseme.tcb");
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		butler = new LUCI_Butler<Integer,String>(bdbl,8181,new TestAccessControl());
		butler.initialize();
		
		try{
			bdbl_remote = new LUCICabinetBDB_Remote<Integer,String>("localhost",8181);
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
			if(bdbl_remote != null){
				bdbl_remote.close();
				bdbl_remote = null;
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
			if(bdbl != null){
				bdbl.close();
				bdbl = null;
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
			bdbl_remote.put(key, value);
		}
			
		for(Integer key=0; key< 1000; key++){
			String x = (String) bdbl_remote.get(key);
			assertEquals("foo"+key,x);
		}
		
		assertEquals(1000,bdbl_remote.size());
		assertEquals(1000,bdbl_remote.sizeLong());
		
		for(Integer key=0; key< 1000; key++){
			bdbl_remote.remove(key);
		}
			
		for(Integer key=0; key< 1000; key++){
			String x = bdbl_remote.get(key);
			assertTrue(x == null);
		}
	}


	@SuppressWarnings("unchecked")
	@Test
	public void testIterate() {
		
		for(Integer key=0; key< 1000; key++){
			String value = "foo"+key;
			bdbl_remote.put(key, value);
		}
		assertEquals(1000,bdbl_remote.size());
			
		IteratorWorkerCountEntries<Integer, String> iw = null;
		try{
			iw = (IteratorWorkerCountEntries<Integer, String>) bdbl_remote.iterate((Class<? extends IteratorWorker<Integer, String>>) IteratorWorkerCountEntries.class,null);
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
						bdbl_remote.put(key, value);
						bdbl_remote.get(key);
					}
					try {
						bdbl_remote.iterateASync((Class<? extends IteratorWorker<Integer, String>>) IteratorWorkerCountEntries.class,null);
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