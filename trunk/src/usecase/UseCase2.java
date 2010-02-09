package usecase;

import edu.uci.ics.luci.lucicabinet.LUCICabinetBDB;
import edu.uci.ics.luci.lucicabinet.LUCICabinetMap;

/**
 *  This class implements Use Case 2
 */
public class UseCase2 {

	public static void main(String[] args) {
		
		final LUCICabinetMap<Integer,String> bdbl = new LUCICabinetBDB<Integer,String>("usecase2.tcb",true);
		
		Runnable r = new Runnable(){
			public void run() {
				for(int j=0; j< 10; j++){
					for(Integer key =0; key < 1000; key++){
						String value = "foo"+key;
						bdbl.put(key, value);
						bdbl.get(key);
					}
				}
				
			}
		};
		
		Thread[] t = new Thread[100];
		for(int i =0; i< 100; i++){
			t[i] = new Thread(r);
		}
		
		long start = System.currentTimeMillis();
		for(int i =0; i< 100; i++){
			t[i].start();
		}
		for(int i =0; i< 100; i++){
			try {
				t[i].join();
			} catch (InterruptedException e) {
			}
		}
		
		double duration = System.currentTimeMillis()-start;
		System.out.println(""+(100*10*1000)+" puts and "+(100*10*1000)+" gets in "+duration+" milliseconds");
		System.out.println(""+(duration/((100*10*1000)+(100*10*1000)))+" milliseconds per operation");
		
		bdbl.close();
	}
}
