package edu.uci.ics.luci.lucicabinet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.luci.lucicabinet.library.IteratorWorkerCountEntries;

public class LUCICabinetMap_Shard_Test {
	
	int numberOfShards = 5;
	List<LUCICabinetMap<Integer,String>> localShards = null;
	List<LUCICabinetMap<Integer,String>> remoteBackingShards = null;
	List<LUCICabinetMap<Integer,String>> remoteShards = null;
	List<LUCI_Butler<Integer,String>> butlers = null;
	
	LUCICabinetMap<Integer,String> sharded_DB = null;

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
		
		localShards = new ArrayList<LUCICabinetMap<Integer,String>>(this.numberOfShards);
		remoteBackingShards = new ArrayList<LUCICabinetMap<Integer,String>>(this.numberOfShards);
		remoteShards = new ArrayList<LUCICabinetMap<Integer,String>>(this.numberOfShards);
		butlers = new ArrayList<LUCI_Butler<Integer,String>>(this.numberOfShards);
		
		try{
			for(int i = 0; i < this.numberOfShards; i++){
				localShards.add(new LUCICabinetHDB<Integer,String>("eraseme"+i+".tch"));
				remoteBackingShards.add(new LUCICabinetHDB<Integer,String>("eraseme"+i+"Remote.tch"));
			}
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		
		try{
			for(int i = 0; i < this.numberOfShards; i++){
				LUCI_Butler<Integer,String> b = new LUCI_Butler<Integer,String>(remoteBackingShards.get(i),8081+i,new TestAccessControl());
				b.initialize();
				butlers.add(b);
			}
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
		try{
			for(int i = 0; i < this.numberOfShards; i++){
				remoteShards.add(new LUCICabinetHDB_Remote<Integer,String>("localhost",8081+i));
			}
		}
		catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		
	}

	@After
	public void tearDown() throws Exception {
		
		if(localShards != null){
			for(int i = 0; i < this.numberOfShards; i++){
				try{
					if(localShards.get(i) != null){
						localShards.get(i).close();
					}
				} catch(RuntimeException e){
					fail("This shouldn't throw an exception"+e);
				}
			}
			localShards.clear();
			localShards=null;
		}
		
		
		if(remoteShards != null){
			for(int i = 0; i < this.numberOfShards; i++){
				try{
					if(remoteShards.get(i) != null){
						remoteShards.get(i).close();
					}
				} catch(RuntimeException e){
					fail("This shouldn't throw an exception"+e);
				}
			}
			remoteShards.clear();
			remoteShards = null;
		}
		
		
		if(butlers != null){
			try{
				for(int i = 0; i < this.numberOfShards; i++){
					if(butlers.get(i) != null){
						butlers.get(i).shutdown();
					}
				}
			} catch(RuntimeException e){
				fail("This shouldn't throw an exception"+e);
			}
			butlers.clear();
			butlers = null;
		}
		
		
		if(remoteBackingShards != null){
			try{
				for(int i = 0; i < this.numberOfShards; i++){
					if(remoteBackingShards.get(i) != null){
						remoteBackingShards.get(i).close();
					}
				}
			}
			catch(RuntimeException e){
				fail("This shouldn't throw an exception"+e);
			}
			remoteBackingShards.clear();
			remoteBackingShards=null;
		}
		
	}


	@Test
	public void testPutGetOut() {
		List<LUCICabinetMap<Integer,String>> shards = new ArrayList<LUCICabinetMap<Integer,String>>(this.numberOfShards*2);
		shards.addAll(localShards);
		shards.addAll(remoteShards);
		sharded_DB = new LUCICabinetMap_Shard<Integer,String>(shards);
		
		for(Integer key=0; key< 1000; key++){
			String value = "foo"+key;
			sharded_DB.put(key, value);
		}
			
		for(Integer key=0; key< 1000; key++){
			String x = (String) sharded_DB.get(key);
			assertEquals("foo"+key,x);
		}
			
		for(Integer key=0; key< 1000; key++){
			sharded_DB.remove(key);
		}
			
		for(Integer key=0; key< 1000; key++){
			Serializable x = sharded_DB.get(key);
			assertTrue(x == null);
		}
		
		sharded_DB.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testIterate() {
		
		List<LUCICabinetMap<Integer,String>> shards = new ArrayList<LUCICabinetMap<Integer,String>>(this.numberOfShards*2);
		shards.addAll(localShards);
		shards.addAll(remoteShards);
		sharded_DB = new LUCICabinetMap_Shard<Integer,String>(shards);
			
		for(Integer key=0; key< 1000; key++){
			String value = "foo"+key;
			sharded_DB.put(key, value);
		}
		assertEquals(1000,sharded_DB.size());
			
		IteratorWorkerCountEntries<Integer,String> iw = null;
		try{
			iw = (IteratorWorkerCountEntries<Integer, String>) sharded_DB.iterate((Class<? extends IteratorWorker<Integer, String>>) IteratorWorkerCountEntries.class,null);
		} catch (InstantiationException e) {
			fail("This shouldn't throw an exception"+e);
		} catch (IllegalAccessException e) {
			fail("This shouldn't throw an exception"+e);
		} catch(RuntimeException e){
			fail("This shouldn't throw an exception"+e);
		}
		assertEquals(1000,iw.count);
		assertEquals(1000,sharded_DB.size());
		
		sharded_DB.close();
	}
		
	@Test
	public void testForDeadlock() {
		List<LUCICabinetMap<Integer,String>> shards = new ArrayList<LUCICabinetMap<Integer,String>>(this.numberOfShards*2);
		shards.addAll(localShards);
		shards.addAll(remoteShards);
		sharded_DB = new LUCICabinetMap_Shard<Integer,String>(shards);
		
		final int number = 75;
		
		/*This finishes if there is no deadlock, this doesn't guarantee no deadlocks can happen though */
		Runnable sharded = new Runnable(){
			@SuppressWarnings("unchecked")
			public void run() {
				for(int j=0; j< 10; j++){
					for(Integer key=0; key< number; key++){
						String value = "foo"+key;
						sharded_DB.put(key, value);
						sharded_DB.get(key);
					}
					try {
						sharded_DB.iterate((Class<? extends IteratorWorker<Integer, String>>) IteratorWorkerCountEntries.class,null);
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
			t[i] = new Thread(sharded);
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
		System.out.println(""+(duration/((threadnumber*10*number)+(threadnumber*10*number)))+" milliseconds per operation");
		testIterate();
	}
}