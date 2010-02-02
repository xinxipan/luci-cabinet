package usecase;

import tokyocabinet.Util;
import edu.uci.ics.luci.lucicabinet.HDB_LUCI;

/**
 *  This class implements Use Case 2
 */
public class UseCase2 {

	public static void main(String[] args) {
		
		final HDB_LUCI hdbl = new HDB_LUCI();
		hdbl.open("eraseme.tch");
		
		Runnable r = new Runnable(){
			public void run() {
				for(int j=0; j< 10; j++){
					for(int i=0; i< 1000; i++){
						byte[] key = Util.packint(i);
						String value = "foo"+i;
						hdbl.put(key, Util.serialize(value));
						hdbl.get(key);
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
		
		hdbl.close();
	}
}
