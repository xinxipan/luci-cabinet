package edu.uci.ics.luci.lucicabinet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.Serializable;
import java.net.UnknownHostException;

import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class BDB_LUCI_RemoteTest {
	
	BDB_LUCI bdbl = null;
	Butler butler = null;
	BDB_LUCI_Remote bdbl_remote = null;

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
			bdbl = new BDB_LUCI("eraseme.tcb");
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		butler = new Butler(bdbl,8181,new TestAccessControl());
		butler.initialize();
		
		try{
			bdbl_remote = new BDB_LUCI_Remote("localhost",8181);
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
			
		for(Integer key=0; key< 1000; key++){
			bdbl_remote.remove(key);
		}
			
		for(Integer key=0; key< 1000; key++){
			Serializable x = bdbl_remote.get(key);
			assertTrue(x == null);
		}
	}

	private static class CountEntry extends IteratorWorker{
		/**
		 * 
		 */
		private static final long serialVersionUID = -6338170023311158958L;
		
		int count = 0 ;
			
		@Override
		protected void initialize(DB_LUCI parent){
			count = 100;
		}
			
		@Override
		protected void iterate(Serializable key,Serializable value) {
			count++;
			assertEquals("foo"+(Integer)key,(String) value);
		}
			
		@Override
		protected void shutdown(DB_LUCI parent){
			count += 1000;
		}
	}

	@Test
	public void testIterate() {
			
		for(Integer key=0; key< 1000; key++){
			String value = "foo"+key;
			bdbl_remote.putSync(key, value);
		}
		assertEquals(1000,bdbl_remote.size());
			
		CountEntry iw = new CountEntry();
		try{
			iw = (CountEntry) bdbl_remote.iterate(iw);
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		assertEquals(1000,bdbl_remote.size());
		
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
		}
		assertEquals(2100,iw.count);
	}
		
	@Test
	public void testForDeadlock() {
		final int number = 50;
		
		/*This finishes if there is no deadlock, this doesn't guarantee no deadlocks can happen though */
		Runnable remote = new Runnable(){
			public void run() {
				for(int j=0; j< 10; j++){
					for(Integer key=0; key< number; key++){
						String value = "foo"+key;
						bdbl_remote.put(key, value);
						bdbl_remote.get(key);
					}
					CountEntry ce = new CountEntry();
					bdbl_remote.iterateAsync(ce);
				}
					
			}
		};
		
		Runnable local = new Runnable(){
			public void run() {
				for(int j=0; j< 10; j++){
					for(Integer key=0; key< number; key++){
						String value = "foo"+key;
						bdbl.put(key, value);
						bdbl.get(key);
					}
					CountEntry ce = new CountEntry();
					bdbl.iterate(ce);
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