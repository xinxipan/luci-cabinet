package usecase;

import edu.uci.ics.luci.lucicabinet.LUCICabinetBDB;
import edu.uci.ics.luci.lucicabinet.LUCICabinetMap;

public class UseCase1b {

	public static void main(String[] args) {
		
		LUCICabinetMap<String,String> bdbl = new LUCICabinetBDB<String,String>("usecase1.tch",true);
		
		String key = "foo";
		String value = 	"bar";

		bdbl.put(key,value);
		System.out.println("Size:"+bdbl.size());
		System.out.println("Value:"+(String)bdbl.get(key));

		bdbl.close();
	}
}
