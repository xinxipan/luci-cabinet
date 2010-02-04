package edu.uci.ics.luci.lucicabinet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.ArrayList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BDB_LUCITest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	BDB_LUCI bdbl = null;
	
	@Before
	public void setUp() throws Exception {
		try{
			bdbl = new BDB_LUCI("eraseme.tcb");
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		bdbl.iterate(new RemoveAll());
	}

	@After
	public void tearDown() throws Exception {
		if(bdbl != null){
			try{
				bdbl.iterate(new RemoveAll());
				bdbl.close();
			}
			catch(RuntimeException e){
				fail("This shouldn't throw an exception"+e);
			}
		}
	}
	
	private static class RemoveAll extends IteratorWorker{
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 3482274413997552663L;
		
		ArrayList<Serializable> removeUs = new ArrayList<Serializable>();
		
		@Override
		protected void iterate(Serializable key,Serializable value) {
			removeUs.add(key);
		}
		
		@Override
		protected void shutdown(DB_LUCI parent){
			for(Serializable key:removeUs){
				parent.remove(key);
			}
		}
		
	}

	@Test
	public void testOpenClose() {
		HDB_LUCI hdb = null;
		try{
			hdb = new HDB_LUCI("eraseme2.tch");
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		hdb.iterate(new RemoveAll());
		
		assertEquals(0,hdb.size());
		
		try{
			hdb.remove("foo".getBytes());
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		try{
			hdb.put("foo".getBytes(),"bar".getBytes());
			hdb.put("foo".getBytes(),"baz".getBytes());
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		assertEquals(1,hdb.size());
		
		hdb.iterate(new RemoveAll());
		
		assertEquals(0,hdb.size());
		
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
			bdbl.put(key,value);
		}
		
		assertEquals(1000,bdbl.size());
		
		for(Integer key=0; key< 1000; key++){
			String x = (String) bdbl.get(key);
			assertEquals("foo"+key,x);
		}
		
		assertEquals(bdbl.size(),1000);
		
		for(Integer key=0; key< 1000; key++){
			bdbl.remove(key);
		}
		
		assertEquals(bdbl.size(),0);
		
		for(Integer key=0; key< 1000; key++){
			String x = (String) bdbl.get(key);
			assertTrue(x == null);
		}
	}

	private static class CountEntry extends IteratorWorker{
		
		private static final long serialVersionUID = -3321514898865348148L;
		
		int count = 0 ;
		
		@Override
		protected void initialize(DB_LUCI parent){
			count = 100;
		}
		
		@Override
		protected void iterate(Serializable key,Serializable value) {
			count++;
			assertEquals("foo"+(Integer)key,(String)value);
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
			bdbl.put(key,value);
		}
		
		CountEntry iw = new CountEntry();
		try{
			bdbl.iterate(iw);
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		assertEquals(2100,iw.count);
		
	}
	
	@Test
	public void testForDeadlock() {
		/*This finishes if there is no deadlock, this doesn't guarantee no deadlocks can happen though */
		Runnable r = new Runnable(){
			public void run() {
				for(int j=0; j< 10; j++){
					for(Integer key=0; key< 1000; key++){
						String value = "foo"+key;
						bdbl.put(key, value);
						bdbl.get(key);
					}
					CountEntry ce = new CountEntry();
					bdbl.iterate(ce);
				}
				
			}
		};
		
		Thread[] t = new Thread[100];
		for(int i =0; i< 100; i++){
			t[i] = new Thread(r);
		}
		
		long start = System.currentTimeMillis();
		for(int i =0; i< 100; i++){
			t[i].start();
		}
		for(int i =0; i< 100; i++){
			try {
				t[i].join();
			} catch (InterruptedException e) {
				fail("This shouldn't be interrupted"+e);
			}
		}
		double duration = System.currentTimeMillis()-start;
		System.out.println(""+(100*10*1000)+" puts and "+(100*10*2000)+" gets in "+duration+" milliseconds");
		System.out.println(""+(duration/((100*10*1000)+(100*10*2000)))+" milliseconds per operation");
		testIterate();
	}
}
