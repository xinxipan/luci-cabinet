package edu.uci.ics.luci.lucicabinet;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.uci.ics.luci.lucicabinet.library.IteratorWorkerFindValue;
import edu.uci.ics.luci.lucicabinet.library.IteratorWorkerHashCode;
import edu.uci.ics.luci.lucicabinet.library.IteratorWorkerMakeKeySet;
import edu.uci.ics.luci.lucicabinet.library.IteratorWorkerMakeValueCollection;
import edu.uci.ics.luci.lucicabinet.library.IteratorWorkerFindValue.FindValueConfig;

/**
 * The abstract class for the databases that luci-cabinet synchronizes over
 *
 */
public abstract class LUCICabinetMap<K extends Serializable,V extends Serializable> implements Map<K,V>{
	
	public abstract boolean getOptimize();
	public abstract void setOptimize(boolean optimize);
	
	public abstract V get(Object key);
	
	public abstract V put(K key, V value);
	
	public abstract V remove(Object key);
	
	public abstract IteratorWorker<K,V> iterate(Class<? extends IteratorWorker<K,V>> iw,IteratorWorkerConfig iwc) throws InstantiationException, IllegalAccessException;
	
	public abstract Long sizeLong();
	
	public abstract void close();
	
	private static transient volatile Logger log = null;
	public static Logger getLog(){
		if(log == null){
			log = Logger.getLogger(LUCICabinetMap.class);
		}
		return log;
	}

	
	public boolean containsKey(Object key){
		return(get(key) != null);
	}
	
	@SuppressWarnings("unchecked")
	public boolean containsValue(Object value){
		FindValueConfig<Object> fvc = new IteratorWorkerFindValue.FindValueConfig<Object>(value);
		IteratorWorkerFindValue<K, V> iw;
		try {
			iw = (IteratorWorkerFindValue<K, V>) iterate((Class<? extends IteratorWorker<K, V>>) IteratorWorkerFindValue.class,fvc);
		} catch (InstantiationException e) {
			return(false);
		} catch (IllegalAccessException e) {
			return(false);
		}
		return(iw.found);
	}
	
	public Set<Entry<K,V>> entrySet(){
		throw new RuntimeException("LUCICabinetMap hasn't implemented entrySet yet, use iterate");
	}
	
	@SuppressWarnings("unchecked")
	public int hashCode(){
		IteratorWorkerHashCode<K, V> iw;
		try {
			iw = (IteratorWorkerHashCode<K, V>) iterate((Class<? extends IteratorWorker<K, V>>) IteratorWorkerHashCode.class,new IteratorWorkerConfig());
		} catch (InstantiationException e) {
			return(0);
		} catch (IllegalAccessException e) {
			return(0);
		}
		return(iw.hashcode);
	}
	
	@SuppressWarnings("unchecked")
	public boolean equals(Object aThat){
		if ( this == aThat ){
			return true;
		}
		else{
			if ( !(aThat instanceof LUCICabinetMap) ){
				return false;
			}
			else{
				LUCICabinetMap<K,V> that = (LUCICabinetMap<K,V>)aThat;
				return(this.hashCode() == that.hashCode());
			}
		}
	}
	
	public boolean isEmpty(){
		return(size() == 0);
	}
	
	@SuppressWarnings("unchecked")
	public Set<K> keySet(){
		IteratorWorkerMakeKeySet<K, V> iw;
		try {
			iw = (IteratorWorkerMakeKeySet<K, V>) iterate((Class<? extends IteratorWorker<K, V>>) IteratorWorkerMakeKeySet.class,new IteratorWorkerConfig());
		} catch (InstantiationException e) {
			return(null);
		} catch (IllegalAccessException e) {
			return(null);
		}
		return(iw.keySet);
	}
	
	public void putAll(Map<? extends K,? extends V> map){
		for(Entry<? extends K, ? extends V> e:map.entrySet()){
			this.put(e.getKey(),e.getValue());
		}
	}

	
	@SuppressWarnings("unchecked")
	public Collection<V> values(){
		IteratorWorkerMakeValueCollection<K, V> iw;
		try {
			iw = (IteratorWorkerMakeValueCollection<K, V>) iterate((Class<? extends IteratorWorker<K, V>>) IteratorWorkerMakeValueCollection.class,new IteratorWorkerConfig());
		} catch (InstantiationException e) {
			return(null);
		} catch (IllegalAccessException e) {
			return(null);
		}
		return(iw.valueCollection);
	}

	/**
	 * Return the number of records in the database. This version returns an int which is for the Map interface.
	 */
	public int size() {
		long ret = sizeLong();
		if(ret > Integer.MAX_VALUE){
			throw new RuntimeException("Database size exceeds max int, use sizeLong");
		}
		return (int) (ret);
	}
	
	
}
