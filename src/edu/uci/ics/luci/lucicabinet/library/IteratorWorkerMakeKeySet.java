package edu.uci.ics.luci.lucicabinet.library;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import edu.uci.ics.luci.lucicabinet.IteratorWorker;

public class IteratorWorkerMakeKeySet<K extends Serializable,V extends Serializable> extends IteratorWorker<K,V>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -854793695220209844L;
	
	public Set<K> keySet = new HashSet<K>();

	@Override
	protected boolean iterate(K key,V value) {
		keySet.add(key);
		return(false); //Don't stop iterating
	}
	
	@Override
	protected void combine(IteratorWorker<K,V> iw) {
		keySet.addAll(((IteratorWorkerMakeKeySet<K,V>)iw).keySet);
	}
	
}