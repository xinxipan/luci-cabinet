package edu.uci.ics.luci.lucicabinet.library;

import java.io.Serializable;

import edu.uci.ics.luci.lucicabinet.IteratorWorker;
import edu.uci.ics.luci.lucicabinet.IteratorWorkerConfig;
import edu.uci.ics.luci.lucicabinet.LUCICabinetMap;

public class IteratorWorkerEquals<K extends Serializable,V extends Serializable> extends IteratorWorker<K,V>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6716359943930408201L;

	public static class EqualsConfig<K1 extends Serializable,V1 extends Serializable> extends IteratorWorkerConfig{
		
		/**
		 * 
		 */
		private static final long serialVersionUID = -318325636439981299L;
		
		LUCICabinetMap<K1,V1> reference = null;
		
		public EqualsConfig(LUCICabinetMap<K1,V1> reference){
			super();
			this.reference = reference;
		}
	}
	
	public boolean equals = true;
	private transient LUCICabinetMap<K,V> reference = null;
	
	@SuppressWarnings("unchecked")
	@Override
	@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="BC_UNCONFIRMED_CAST", justification="It's the only way to serialize this")
	protected void initialize(LUCICabinetMap<K,V> parent,IteratorWorkerConfig iwc){ 
		this.reference = ((EqualsConfig<K,V>) iwc).reference;
	}

	@Override
	protected boolean iterate(K key,V value) {
		if(equals){
			if(reference == null){
				equals = false;
			}
			else{
				V referenceValue = reference.get(key);
				if(value == null){
					if(referenceValue != null){
						equals = false;
					}
				}
				else{
					if(referenceValue == null){
						equals = false;
					}
					else{
						if(!value.equals(referenceValue)){
							equals = false;
						}
					}
				}
			}
		}
		return(!equals);
	}
	
	@Override
	protected void combine(IteratorWorker<K,V> iw) {
		equals &= ((IteratorWorkerEquals<K,V>)iw).equals;
	}
	
}