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

import tokyocabinet.Util;


public class HDB_LUCI_RemoteTest {
	
	HDB_LUCI hdb = null;
	Butler butler = null;
	HDB_LUCI_Remote hdb_remote = null;

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
		hdb = new HDB_LUCI();
		
		try{
			hdb.open("eraseme.tch");
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		butler = new Butler(hdb,8181,new TestAccessControl());
		butler.initialize();
		
		try{
			hdb_remote = new HDB_LUCI_Remote("localhost",8181);
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
			if(hdb != null){
				hdb.close();
				hdb = null;
			}
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
	}


	@Test
	public void testPutGetOut() {
		for(int i=0; i< 1000; i++){
			byte[] key = Util.packint(i);
			String value = "foo"+i;
			
			hdb_remote.put(key, Util.serialize(value));
		}
			
		for(int i=0; i< 1000; i++){
			byte[] key = Util.packint(i);
			byte[] x = hdb_remote.get(key);
			String s = (String) Util.deserialize(x);
			assertEquals("foo"+i,s);
		}
			
		for(int i=0; i< 1000; i++){
			byte[] key = Util.packint(i);
			hdb_remote.remove(key);
		}
			
		for(int i=0; i< 1000; i++){
			byte[] key = Util.packint(i);
			byte[] x = hdb_remote.get(key);
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
		protected void initialize(HDB_LUCI parent){
			count = 100;
		}
			
		@Override
		protected void iterate(byte[] key,byte[] value) {
			count++;
			String s = (String) Util.deserialize(value);
			assertEquals("foo"+(Integer)Util.unpackint(key),s);
		}
			
		@Override
		protected void shutdown(HDB_LUCI parent){
			count += 1000;
		}
	}

	@Test
	public void testIterate() {
			
		for(int i=0; i< 1000; i++){
			byte[] key = Util.packint(i);
			String value = "foo"+i;
			hdb_remote.put(key, Util.serialize(value));
		}
		assertEquals(1000,hdb_remote.size());
			
		CountEntry iw = new CountEntry();
		try{
			iw = (CountEntry) hdb_remote.iterateSync(iw);
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		assertEquals(1000,hdb_remote.size());
		
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
		}
		assertEquals(2100,iw.count);
	}
		
	@Test
	public void testForDeadlock() {
		final int number = 250;
		
		/*This finishes if there is no deadlock, this doesn't guarantee no deadlocks can happen though */
		Runnable remote = new Runnable(){
			public void run() {
				for(int j=0; j< 10; j++){
					for(int i=0; i< number; i++){
						byte[] key = Util.packint(i);
						String value = "foo"+i;
						hdb_remote.put(key, Util.serialize(value));
						hdb_remote.get(key);
					}
					CountEntry ce = new CountEntry();
					hdb_remote.iterate(ce);
				}
					
			}
		};
		
		Runnable local = new Runnable(){
			public void run() {
				for(int j=0; j< 10; j++){
					for(int i=0; i< number; i++){
						byte[] key = Util.packint(i);
						String value = "foo"+i;
						hdb.put(key, Util.serialize(value));
						hdb.get(key);
					}
					CountEntry ce = new CountEntry();
					hdb.iterate(ce);
				}
					
			}
		};
			
		Thread[] t = new Thread[200];
		for(int i =0; i< 200; i+=2){
			t[i] = new Thread(remote);
			t[i+1] = new Thread(local);
		}
			
		long start = System.currentTimeMillis();
		for(int i =0; i< 200; i++){
			t[i].start();
		}
		for(int i =0; i< 200; i++){
			try {
				t[i].join();
			} catch (InterruptedException e) {
				fail("This shouldn't be interrupted"+e);
			}
		}
		
		double duration = System.currentTimeMillis()-start;
		System.out.println(""+(100*10*number)+" local puts, "+(100*10*number)+" remote puts, "+(100*10*number)+" local gets, and "+(100*10*number)+" remote gets in "+duration+" milliseconds");
		System.out.println(""+(duration/(2*((100*10*number)+(100*10*number))))+" milliseconds per operation");
		testIterate();
	}
}