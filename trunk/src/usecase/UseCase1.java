package usecase;

import tokyocabinet.Util;
import edu.uci.ics.luci.lucicabinet.HDB_LUCI;

/**
 *  This class implements Use Case 1
 */
public class UseCase1 {

	public static void main(String[] args) {
		
		HDB_LUCI hdbl = new HDB_LUCI();
		hdbl.open("eraseme.tch");
		
		byte[] key = Util.serialize("foo");
		byte[] value = 	Util.serialize("bar");

		hdbl.put(key,value);
		System.out.println("Size:"+hdbl.size());
		System.out.println("Value:"+(String)Util.deserialize(hdbl.get(key)));

		hdbl.close();
	}
}
