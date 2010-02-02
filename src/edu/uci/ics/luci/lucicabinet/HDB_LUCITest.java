package edu.uci.ics.luci.lucicabinet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tokyocabinet.Util;

public class HDB_LUCITest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	HDB_LUCI hdb = new HDB_LUCI();
	
	@Before
	public void setUp() throws Exception {
		assertTrue(hdb != null);
		try{
			hdb.open("eraseme.tch");
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		hdb.iterate(new RemoveAll());
	}

	@After
	public void tearDown() throws Exception {
		hdb.iterate(new RemoveAll());

		try{
			hdb.close();
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
	}
	
	private static class RemoveAll extends IteratorWorker{
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 8559552119218888148L;
		
		ArrayList<byte[]> removeUs = new ArrayList<byte[]>();
		
		@Override
		protected void iterate(byte[] key,byte[] value) {
			removeUs.add(key);
		}
		
		@Override
		protected void shutdown(HDB_LUCI parent){
			for(byte[] key:removeUs){
				parent.remove(key);
			}
		}
		
	}

	@Test
	public void testOpenClose() {
		HDB_LUCI hdb = new HDB_LUCI();
		try{
			hdb.open("eraseme2.tch");
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
		
		
		try{
			hdb.close();
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
		
			hdb.put(key, Util.serialize(value));
		}
		Util.serialize("foo");
		
		assertEquals(hdb.size(),1000);
		
		for(int i=0; i< 1000; i++){
			byte[] key = Util.packint(i);
			byte[] x = hdb.get(key);
			String s = (String) Util.deserialize(x);
			assertEquals("foo"+i,s);
		}
		
		assertEquals(hdb.size(),1000);
		
		for(int i=0; i< 1000; i++){
			byte[] key = Util.packint(i);
			hdb.remove(key);
		}
		
		assertEquals(hdb.size(),0);
		
		for(int i=0; i< 1000; i++){
			byte[] key = Util.packint(i);
			byte[] x = hdb.get(key);
			assertTrue(x == null);
		}
	}

	private static class CountEntry extends IteratorWorker{
		
		private static final long serialVersionUID = -3321514898865348148L;
		
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
			hdb.put(key, Util.serialize(value));
		}
		
		CountEntry iw = new CountEntry();
		try{
			hdb.iterate(iw);
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
					for(int i=0; i< 1000; i++){
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
