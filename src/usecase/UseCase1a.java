package usecase;

import edu.uci.ics.luci.lucicabinet.LUCICabinetHDB;
import edu.uci.ics.luci.lucicabinet.LUCICabinetMap;

public class UseCase1a {

	public static void main(String[] args) {
		
		LUCICabinetMap<String,String> hdbl = new LUCICabinetHDB<String,String>("usecase1.tch",true);
		
		String key = "foo";
		String value = 	"bar";

		hdbl.put(key,value);
		System.out.println("Size:"+hdbl.size());
		System.out.println("Value:"+(String)hdbl.get(key));

		hdbl.close();
	}
}
