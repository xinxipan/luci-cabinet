package edu.uci.ics.luci.lucicabinet.library;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.luci.lucicabinet.IteratorWorker;
import edu.uci.ics.luci.lucicabinet.LUCICabinetMap;

public class IteratorWorkerRemoveAll<K extends Serializable,V extends Serializable> extends IteratorWorker<K,V>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8970412061680511541L;
	
	private List<K> removeUs = new ArrayList<K>();

	@Override
	protected boolean iterate(K key,V value) {
		removeUs.add(key);
		return(false); //Don't stop iterating
	}
	
	@Override
	protected void shutdown(LUCICabinetMap<K,V> parent){
		for(K key:removeUs){
			parent.remove(key);
		}
	}

	@Override
	protected void combine(IteratorWorker<K,V> iw) {
	}
	
}