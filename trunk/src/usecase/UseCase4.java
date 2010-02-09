package usecase;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.UnknownHostException;

import edu.uci.ics.luci.lucicabinet.AccessControl;
import edu.uci.ics.luci.lucicabinet.LUCICabinetHDB;
import edu.uci.ics.luci.lucicabinet.LUCICabinetHDB_Remote;
import edu.uci.ics.luci.lucicabinet.LUCICabinetMap;
import edu.uci.ics.luci.lucicabinet.LUCI_Butler;

/**
 *  This class implements Use Case 4
 */
public class UseCase4 {
	
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


	public static void main(String[] args) throws UnknownHostException, IOException {
		
		/* Set up the server side database */
		final LUCICabinetMap<Integer,String> hdbl = new LUCICabinetHDB<Integer,String>("usecase4.tch",true);
		
		/* Create a service to receive commands on port 8181 */
		LUCI_Butler<Integer,String> butler = new LUCI_Butler<Integer,String>(hdbl,8181,new TestAccessControl());
		butler.initialize();
		
		/* Create the client side database interface which will talk over sockets to the 
		 * server side database */
		
		final LUCICabinetHDB_Remote<Integer,String> hdbl_remote = new LUCICabinetHDB_Remote<Integer,String>("localhost",8181,true);
		
		final int number = 75;
		
		/*This finishes if there is no deadlock, this doesn't guarantee no deadlocks can happen though */
		Runnable remote = new Runnable(){
			public void run() {
				for(int j=0; j< 10; j++){
					for(Integer key=0; key< number; key++){
						String value = "foo"+key;
						hdbl_remote.put(key, value);
						hdbl_remote.get(key);
					}
				}
					
			}
		};
		
		Runnable local = new Runnable(){
			public void run() {
				for(int j=0; j< 10; j++){
					for(Integer key=0; key< number; key++){
						String value = "foo"+key;
						hdbl_remote.put(key, value);
						hdbl_remote.get(key);
					}
				}
					
			}
		};
			
		final int threadnumber = 10;
		Thread[] t = new Thread[threadnumber];
		for(int i =0; i< threadnumber; i+=2){
			t[i] = new Thread(remote);
			t[i+1] = new Thread(local);
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
		System.out.println(""+(duration/(2*((threadnumber*10*number)+(threadnumber*10*number))))+" milliseconds per operation");

		/*Clean up client side */
		hdbl_remote.close();
		
		/*Clean up butler*/
		butler.shutdown();
		
		/*Clean up server side */
		hdbl.close();
	}
}
