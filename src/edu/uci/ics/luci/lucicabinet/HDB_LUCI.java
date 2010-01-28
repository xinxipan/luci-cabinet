package edu.uci.ics.luci.lucicabinet;

import tokyocabinet.HDB;

public class HDB_LUCI{
	
	private HDB hdb = null;

	public HDB_LUCI() {
		super();
		hdb = new HDB();
	}
	
	public synchronized boolean open(String path){
		return(hdb.open(path,HDB.OWRITER | HDB.OCREAT));
	}
	
	public synchronized int ecode(){
		return(hdb.ecode());
	}
	
	public synchronized boolean out(byte[] key){
		return(hdb.out(key));
	}
	
	public synchronized boolean remove(byte[] key){
		return(out(key));
	}
	
	public synchronized boolean put(byte[] key, byte[] value){
		return(hdb.put(key,value));
	}
	
	public synchronized byte[] get(byte[] key){
		return(hdb.get(key));
	}
	
	public synchronized boolean iterate(IteratorWorker iw){
		byte[] key;
		boolean x = hdb.iterinit();
		if(x){
			while ((key = hdb.iternext()) != null) {
				iw.doWork(this,key);
			}
		}
		return(x);
	}
	
	public synchronized boolean close(){
		return(hdb.close());
	}
	
}