package edu.uci.ics.luci.lucicabinet.library;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import edu.uci.ics.luci.lucicabinet.IteratorWorker;

public class IteratorWorkerHashCode<K extends Serializable,V extends Serializable> extends IteratorWorker<K,V>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6514074855861620655L;
	
	public int hashcode;
	public Map<K,V> temp = new HashMap<K,V>(1);
	
	@Override
	protected boolean iterate(K key,V value) {
		/*This is an attempt to keep the map contract of using the sum of the hashcodes of the entry set */
		temp.put(key,value);
		for(Entry<K,V> e:temp.entrySet()){
			hashcode += e.hashCode();
		}
		temp.clear();
		return(false); // Don't stop iterating
	}
	
	@Override
	protected void combine(IteratorWorker<K,V> iw) {
		hashcode += ((IteratorWorkerHashCode<K,V>)iw).hashcode;
	}
	
}