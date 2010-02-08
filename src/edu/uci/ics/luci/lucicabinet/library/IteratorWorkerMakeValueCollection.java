package edu.uci.ics.luci.lucicabinet.library;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import edu.uci.ics.luci.lucicabinet.IteratorWorker;

public class IteratorWorkerMakeValueCollection<K extends Serializable,V extends Serializable> extends IteratorWorker<K,V>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1258552450606262290L;
	
	public Collection<V> valueCollection = new ArrayList<V>();

	@Override
	protected boolean iterate(K key,V value) {
		valueCollection.add(value);
		return(false); //Don't stop iterating
	}
	
	@Override
	protected void combine(IteratorWorker<K,V> iw) {
		valueCollection.addAll(((IteratorWorkerMakeValueCollection<K,V>)iw).valueCollection);
	}
	
}