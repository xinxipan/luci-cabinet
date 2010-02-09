package usecase;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import edu.uci.ics.luci.lucicabinet.LUCICabinetHDB;
import edu.uci.ics.luci.lucicabinet.LUCICabinetHDB_Remote;
import edu.uci.ics.luci.lucicabinet.LUCICabinetMap;
import edu.uci.ics.luci.lucicabinet.LUCI_Butler;
import edu.uci.ics.luci.lucicabinet.library.SimpleAccessControl;

/**
 *  This class implements Use Case 3
 */
public class UseCase3 {
	
	public static void main(String[] args) {
		
		/* Set up the server side database */
		final LUCICabinetMap<String,String> dbl = new LUCICabinetHDB<String,String>("usecase3.tch",true);
		
		/* Create a service to receive commands on port 8181 */
		Set<String> allowedConnections = new HashSet<String>(1);
		allowedConnections.add("/127.0.0.1");
		
		LUCI_Butler<String,String> butler = new LUCI_Butler<String,String>(dbl,8181,new SimpleAccessControl(allowedConnections));
		butler.initialize();
		
		/* Create the client side database interface which will talk over sockets to the 
		 * server side database */
		LUCICabinetHDB_Remote<String,String> hdbl_remote = null;
		try {
			hdbl_remote = new LUCICabinetHDB_Remote<String,String>("localhost",8181,true);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return;
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		String key = "foo";
		String value = 	"bar";

		hdbl_remote.putSync(key,value);
		System.out.println("Size:"+hdbl_remote.size());
		System.out.println("Value:"+(String)hdbl_remote.get(key));

		/*Clean up client side */
		hdbl_remote.close();
		
		/*Clean up butler*/
		butler.shutdown();
		
		/*Clean up server side */
		dbl.close();
	}
}
