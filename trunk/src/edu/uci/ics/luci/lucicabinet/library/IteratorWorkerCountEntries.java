package edu.uci.ics.luci.lucicabinet.library;

import java.io.Serializable;

import edu.uci.ics.luci.lucicabinet.IteratorWorker;
import edu.uci.ics.luci.lucicabinet.IteratorWorkerConfig;
import edu.uci.ics.luci.lucicabinet.LUCICabinetMap;

public class IteratorWorkerCountEntries<K extends Serializable,V extends Serializable> extends IteratorWorker<K,V>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 9002734806229671158L;
	
	public int count = 0;
	public boolean ranInit = false;
	public boolean ranShutdown = false;
	
	@Override
	protected void initialize(LUCICabinetMap<K,V> parent,IteratorWorkerConfig iwc){
		ranInit = true;
	}
	
	@Override
	protected boolean iterate(K key,V value) {
		count++;
		return(false); //Keep iterating
	}
	
	@Override
	protected void shutdown(LUCICabinetMap<K,V> parent){
		ranShutdown = true;
	}

	@Override
	protected void combine(IteratorWorker<K,V> iw) {
		count += ((IteratorWorkerCountEntries<K,V>)iw).count;
	}
}