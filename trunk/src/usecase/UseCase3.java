package usecase;

import java.io.IOException;
import java.net.UnknownHostException;

import edu.uci.ics.luci.lucicabinet.AccessControl;
import edu.uci.ics.luci.lucicabinet.Butler;
import edu.uci.ics.luci.lucicabinet.DB_LUCI;
import edu.uci.ics.luci.lucicabinet.HDB_LUCI;
import edu.uci.ics.luci.lucicabinet.HDB_LUCI_Remote;

/**
 *  This class implements Use Case 3
 */
public class UseCase3 {
	
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


	public static void main(String[] args) {
		
		/* Set up the server side database */
		final DB_LUCI dbl = new HDB_LUCI();
		
		/* Open the server side database */
		dbl.open("usecase3.tch");
		
		/* Create a service to receive commands on port 8181 */
		Butler butler = new Butler(dbl,8181,new TestAccessControl());
		butler.initialize();
		
		/* Create the client side database interface which will talk over sockets to the 
		 * server side database */
		HDB_LUCI_Remote hdbl_remote = null;
		try {
			hdbl_remote = new HDB_LUCI_Remote("localhost",8181);
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
