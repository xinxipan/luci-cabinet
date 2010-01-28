package edu.uci.ics.luci.lucicabinet;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tokyocabinet.HDB;
import tokyocabinet.Util;

public class HDB_LUCITest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testOpenClose() {
		HDB_LUCI hdb = new HDB_LUCI();
		assertTrue(hdb != null);
		assertTrue(hdb.open("eraseme.tch"));
		assertTrue(hdb.close());
	}

	@Test
	public void testPutGetOut() {
		HDB_LUCI hdb = new HDB_LUCI();
		assertTrue(hdb != null);
		assertTrue(hdb.open("eraseme.tch"));
		
		
		for(int i=0; i< 1000; i++){
			byte[] key = Util.packint(i);
			String value = "foo"+i;
		
			hdb.put(key, Util.serialize(value));
		}
		
		for(int i=0; i< 1000; i++){
			byte[] key = Util.packint(i);
			byte[] x = hdb.get(key);
			String s = (String) Util.deserialize(x);
			assertEquals("foo"+i,s);
		}
		
		for(int i=0; i< 1000; i++){
			byte[] key = Util.packint(i);
			hdb.remove(key);
		}
		
		for(int i=0; i< 1000; i++){
			byte[] key = Util.packint(i);
			byte[] x = hdb.get(key);
			assertTrue(x == null);
			assertEquals(HDB.ENOREC,hdb.ecode());
		}
		assertTrue(hdb.close());
	}

	private class CountEntry implements IteratorWorker{
		
		int count = 0 ;

		public void doWork(HDB_LUCI db, byte[] key) {
			count++;
			byte[] x = db.get(key);
			assertEquals(0,db.ecode());
			String s = (String) Util.deserialize(x);
			assertEquals("foo"+(Integer)Util.unpackint(key),s);
		}
		
	}

	@Test
	public void testIterate() {
		HDB_LUCI hdb = new HDB_LUCI();
		assertTrue(hdb != null);
		assertTrue(hdb.open("eraseme.tch"));
		
		
		for(int i=0; i< 1000; i++){
			byte[] key = Util.packint(i);
			String value = "foo"+i;
			hdb.put(key, Util.serialize(value));
			assertEquals(0,hdb.ecode());
		}
		
		CountEntry iw = new CountEntry();
		assertTrue(hdb.iterate(iw));
		assertEquals(1000,iw.count);
		
		assertTrue(hdb.close());
	}
}
