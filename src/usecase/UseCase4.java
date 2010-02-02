package usecase;

import java.io.IOException;
import java.net.UnknownHostException;

import tokyocabinet.Util;
import edu.uci.ics.luci.lucicabinet.AccessControl;
import edu.uci.ics.luci.lucicabinet.Butler;
import edu.uci.ics.luci.lucicabinet.HDB_LUCI;
import edu.uci.ics.luci.lucicabinet.HDB_LUCI_Remote;

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
		final HDB_LUCI hdbl = new HDB_LUCI();
		
		/* Open the server side database */
		hdbl.open("eraseme.tch");
		
		/* Create a service to receive commands on port 8181 */
		Butler butler = new Butler(hdbl,8181,new TestAccessControl());
		butler.initialize();
		
		/* Create the client side database interface which will talk over sockets to the 
		 * server side database */
		
		final HDB_LUCI_Remote hdbl_remote = new HDB_LUCI_Remote("localhost",8181);
		
		final int number = 250;
		
		/*This finishes if there is no deadlock, this doesn't guarantee no deadlocks can happen though */
		Runnable remote = new Runnable(){
			public void run() {
				for(int j=0; j< 10; j++){
					for(int i=0; i< number; i++){
						byte[] key = Util.packint(i);
						String value = "foo"+i;
						hdbl_remote.put(key, Util.serialize(value));
						hdbl_remote.get(key);
					}
				}
					
			}
		};
		
		Runnable local = new Runnable(){
			public void run() {
				for(int j=0; j< 10; j++){
					for(int i=0; i< number; i++){
						byte[] key = Util.packint(i);
						String value = "foo"+i;
						hdbl.put(key, Util.serialize(value));
						hdbl.get(key);
					}
				}
					
			}
		};
			
		Thread[] t = new Thread[200];
		for(int i =0; i< 200; i+=2){
			t[i] = new Thread(remote);
			t[i+1] = new Thread(local);
		}
			
		long start = System.currentTimeMillis();
		for(int i =0; i< 200; i++){
			t[i].start();
		}
		for(int i =0; i< 200; i++){
			try {
				t[i].join();
			} catch (InterruptedException e) {
			}
		}
		
		double duration = System.currentTimeMillis()-start;
		System.out.println(""+(100*10*number)+" local puts, "+(100*10*number)+" remote puts, "+(100*10*number)+" local gets, and "+(100*10*number)+" remote gets in "+duration+" milliseconds");
		System.out.println(""+(duration/(2*((100*10*number)+(100*10*number))))+" milliseconds per operation");

		/*Clean up client side */
		hdbl_remote.close();
		
		/*Clean up butler*/
		butler.shutdown();
		
		/*Clean up server side */
		hdbl.close();
	}
}
