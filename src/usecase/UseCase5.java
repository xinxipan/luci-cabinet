package usecase;

import java.util.ArrayList;

import edu.uci.ics.luci.lucicabinet.LUCICabinetBDB;
import edu.uci.ics.luci.lucicabinet.LUCICabinetMap;
import edu.uci.ics.luci.lucicabinet.LUCICabinetMap_Shard;

public class UseCase5 {

	public static void main(String[] args) {
		
		int numberOfShards = 10;
		boolean optimize = true;

		ArrayList<LUCICabinetMap<Integer, String>> localShards = new ArrayList<LUCICabinetMap<Integer,String>>(numberOfShards);
		
		for(int i = 0; i < numberOfShards; i++){
			localShards.add(new LUCICabinetBDB<Integer,String>("usecase"+i+".tcb",optimize));
		}
		
		LUCICabinetMap<Integer, String> sharded_DB = new LUCICabinetMap_Shard<Integer,String>(localShards,optimize);

		
		for(Integer key =0; key < 1000; key++){
			String value = "foo "+key;
			sharded_DB.put(key, value);
			if(key % 100 == 0){
				System.out.println("key:"+key+", value:"+sharded_DB.get(key));
			}
		}
		
		sharded_DB.close();
	}
}
