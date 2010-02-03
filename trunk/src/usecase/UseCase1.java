package usecase;

import edu.uci.ics.luci.lucicabinet.HDB_LUCI;

/**
 *  This class implements Use Case 1
 */
public class UseCase1 {

	public static void main(String[] args) {
		
		HDB_LUCI hdbl = new HDB_LUCI();
		hdbl.open("usecase1.tch");
		
		String key = "foo";
		String value = 	"bar";

		hdbl.put(key,value);
		System.out.println("Size:"+hdbl.size());
		System.out.println("Value:"+(String)hdbl.get(key));

		hdbl.close();
	}
}
