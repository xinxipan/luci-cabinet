package usecase;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.BasicConfigurator;

import edu.uci.ics.luci.lucicabinet.LUCICabinetBDB;
import edu.uci.ics.luci.lucicabinet.LUCICabinetBDB_Remote;
import edu.uci.ics.luci.lucicabinet.LUCICabinetHDB;
import edu.uci.ics.luci.lucicabinet.LUCICabinetMap;
import edu.uci.ics.luci.lucicabinet.LUCICabinetMap_Remote;
import edu.uci.ics.luci.lucicabinet.LUCICabinetMap_Shard;
import edu.uci.ics.luci.lucicabinet.LUCI_Butler;
import edu.uci.ics.luci.lucicabinet.library.SimplestAccessControl;

public class PerformanceTest {
	final static int numberOfShards = 10;
	final static int max = 1000000;

	private static void runTest(LUCICabinetMap<Integer, Integer> db) {
		if(db != null){
			System.out.println("\tClearing database");
			db.clear();
		
			System.out.println("\tRunning write test");
			Long start = System.currentTimeMillis();
			for(int i=0;i<max;i++){
				db.put(i,i);
			}
			db.get(1); //Force writes to complete
			
			System.out.println("\t\twrite test ("+max+" ops) : "+(System.currentTimeMillis()-start)+" milliseconds");
	
			start = System.currentTimeMillis();
			for(int i=0;i<max;i++){
				db.get(i);
			}
			System.out.println("\t\tread test ("+max+" ops) : "+(System.currentTimeMillis()-start)+" milliseconds");
		
			System.out.println("\tClearing database");
			db.clear();
			System.out.println("\tClosing database");
			db.close();
		}
	}
	
	private static void testHDB(){
		LUCICabinetMap<Integer,Integer> db = new LUCICabinetHDB<Integer,Integer>("performance.tch",true);
	
		System.out.println("HDB Test:");
		runTest(db);
	}

	
	private static void testBDB(){
		LUCICabinetMap<Integer, Integer> db = new LUCICabinetBDB<Integer,Integer>("performance.tcb",true);
		
		System.out.println("BDB Test:");
		runTest(db);
	}
	
	private static void testMap_Remote(){

		LUCICabinetMap<Integer, Integer> db = new LUCICabinetHDB<Integer,Integer>("performance.tch",true);
		
		/* Create a service to receive commands on port 8181 */
		Set<String> allowedConnections = new HashSet<String>(1);
		allowedConnections.add("/127.0.0.1");
		
		LUCI_Butler<Integer,Integer> butler = new LUCI_Butler<Integer,Integer>(db,8181,new SimplestAccessControl());
		butler.initialize();
		
		LUCICabinetMap_Remote<Integer, Integer> remote_db=null;
		try {
			remote_db = new LUCICabinetBDB_Remote<Integer,Integer>("localhost",8181,true);
		} catch (UnknownHostException e) {
			System.out.println("fail");
		} catch (IOException e) {
			System.out.println("fail");
		}
		
		System.out.println("Map_Remote Test:");
		runTest(remote_db);
		
		butler.shutdown();
	}
	
	private static void testMap_Shard_Local_HDB(){
		
		ArrayList<LUCICabinetMap<Integer, Integer>> localShards = new ArrayList<LUCICabinetMap<Integer,Integer>>(numberOfShards);
		for(int i = 0; i < numberOfShards; i++){
			localShards.add(new LUCICabinetHDB<Integer,Integer>("performance"+i+".tch",true));
		}

		LUCICabinetMap<Integer, Integer> db = new LUCICabinetMap_Shard<Integer,Integer>(localShards,true);
		
		System.out.println("Map_Shard_Local_HDB Test:");
		runTest(db);
		
	}

	private static void testMap_Shard_Local_BDB(){
		
		ArrayList<LUCICabinetMap<Integer, Integer>> localShards = new ArrayList<LUCICabinetMap<Integer,Integer>>(numberOfShards);
		for(int i = 0; i < numberOfShards; i++){
			localShards.add(new LUCICabinetBDB<Integer,Integer>("performance"+i+".tcb",true));
		}

		LUCICabinetMap<Integer, Integer> db = new LUCICabinetMap_Shard<Integer,Integer>(localShards,true);
		
		System.out.println("Map_Shard_Local_BDB Test:");
		runTest(db);
		
	}
	
	private static void testMap_Shard_Remote(){
		
		ArrayList<LUCICabinetMap<Integer, Integer>> localShards = new ArrayList<LUCICabinetMap<Integer,Integer>>(numberOfShards);
		ArrayList<LUCICabinetMap<Integer, Integer>> remoteShards = new ArrayList<LUCICabinetMap<Integer,Integer>>(numberOfShards);
		ArrayList<LUCI_Butler<Integer, Integer>> butlers = new ArrayList<LUCI_Butler<Integer,Integer>>(numberOfShards);
		for(int i = 0; i < numberOfShards; i++){
			LUCICabinetBDB<Integer, Integer> thing = new LUCICabinetBDB<Integer,Integer>("performance"+i+".tcb",true);
			localShards.add(thing);
			LUCI_Butler<Integer,Integer> b = new LUCI_Butler<Integer,Integer>(thing,8081+i,new SimplestAccessControl());
			b.initialize();
			butlers.add(b);
			try {
				remoteShards.add(new LUCICabinetBDB_Remote<Integer,Integer>("localhost",8081+i,true));
			} catch (UnknownHostException e) {
				System.out.println("fail");
			} catch (IOException e) {
				System.out.println("fail");
			}
		}

		LUCICabinetMap<Integer, Integer> db = new LUCICabinetMap_Shard<Integer,Integer>(remoteShards,true);
		
		System.out.println("Map_Shard_Remote Test:");
		runTest(db);
		
		for(LUCI_Butler<Integer,Integer> b:butlers){
			b.shutdown();
		}
	}

	public static void main(String[] args) {
		BasicConfigurator.configure();

		testHDB();
		testBDB();
		testMap_Remote();
		testMap_Shard_Local_HDB();
		testMap_Shard_Local_BDB();
		testMap_Shard_Remote();
	}
}
