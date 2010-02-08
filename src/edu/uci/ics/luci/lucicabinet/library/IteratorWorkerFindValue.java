package edu.uci.ics.luci.lucicabinet.library;

import java.io.Serializable;

import edu.uci.ics.luci.lucicabinet.IteratorWorker;
import edu.uci.ics.luci.lucicabinet.IteratorWorkerConfig;
import edu.uci.ics.luci.lucicabinet.LUCICabinetMap;

public class IteratorWorkerFindValue<K extends Serializable,V extends Serializable> extends IteratorWorker<K,V>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2439724816812410810L;

	public static class FindValueConfig<T> extends IteratorWorkerConfig{
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 7788014601686482433L;
		
		public T target;
		public FindValueConfig(T target){
			super();
			this.target = target;
		}
	}
	
	public boolean found = false;
	private V target;
	
	@SuppressWarnings("unchecked")
	@Override
	@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="BC_UNCONFIRMED_CAST", justification="It's the only way to serialize this")
	protected void initialize(LUCICabinetMap<K,V> parent,IteratorWorkerConfig iwc){ 
		this.target = ((FindValueConfig<V>)iwc).target;
	}

	@Override
	protected boolean iterate(K key,V value) {
		if(!found){
			if(target == null){
				if(value == null){
					found = true;
				}
			}
			else{
				found |= (target.equals(value));
			}
		}
		return(found);
	}
	
	@Override
	protected void combine(IteratorWorker<K,V> iw) {
		found |= ((IteratorWorkerFindValue<K,V>)iw).found;
	}
	
}