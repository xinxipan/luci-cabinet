package edu.uci.ics.luci.lucicabinet;

import java.io.Serializable;
import java.util.List;

import org.apache.log4j.Logger;

import edu.uci.ics.luci.lucicabinet.library.ShardFunctionSimple;

/**
 * This class implements a sharded database. This means that key-value pairs are stored in a collection of different databases
 * based on a function of their key.  A sharded database speeds up concurrent access and enables scaling to
 * large databases (in theory).
 * 
 */
public class LUCICabinetMap_Shard<K extends Serializable,V extends Serializable> extends LUCICabinetMap<K,V>{

	private List<LUCICabinetMap<K,V>> shards = null;
	private ShardFunction shardFunction;

	private boolean optimize = true;
	
	private static transient volatile Logger log = null;
	public static Logger getLog(){
		if(log == null){
			log = Logger.getLogger(LUCICabinetMap_Shard.class);
		}
		return log;
	}
	
	/**
	 * @param shards A list of databases to shard across.  If any of these databases are the same database, or are
	 * remote wrappers for the same database, then iterations will go across each copy of the database one time
	 * (effectively iterating more than once across entries) which is probably not what you want.
	 * The sharding function is the default based on a modulo of the hash function.
	 * @param optimize If the database is "optimized" then put and removes will be non-blocking and will always return null.
     * This is a violation of the java Map contract, but cuts the database operations in half.
	 * 
	 */
	public LUCICabinetMap_Shard(List<LUCICabinetMap<K,V>> shards,boolean optimize) {
		this(shards,new ShardFunctionSimple(shards.size()),optimize);
	}
	
	/**
	 * 
	 * @param shard A list of databases to shard across.  If any of these databases are the same database, or are
	 * remote wrappers for the same database, then iterations will go across each copy of the database one time
	 * (effectively iterating more than once across entries) which is probably not what you want.
	 * @param sf An object which defines how to associate keys with shards.
	 * @param optimize If the database is "optimized" then put and removes will be non-blocking and will always return null.
     * This is a violation of the java Map contract, but cuts the database operations in half.
	 */
	public LUCICabinetMap_Shard(List<LUCICabinetMap<K,V>> shard,ShardFunction sf,boolean optimize) {
		super();
		
		this.shards = shard;
		this.shardFunction = sf;
		this.optimize = optimize;
	}
	

	/**
    * Getter for the optimize setting of this database
	*/
	@Override
	public synchronized boolean getOptimize() {
		return optimize;
	}
	

	/**
	 * Set optimization for the sharded database.
	 */
	@Override
	public synchronized void setOptimize(boolean optimize){
		this.optimize = optimize;

		for(LUCICabinetMap<K,V> shard:shards){
			shard.setOptimize(optimize);
		}
	}
	
	
	
	/**
	 * Close all sharded databases. 
	 */
	@Override
	public synchronized void close() {
		/* Close the shards */ 
		if(shards != null){
			for(LUCICabinetMap<K,V> db:shards){
				db.close();
			}
		
			shards.clear();
			shards = null;
		}
	}
	

	/** Wrapper for close to make sure all resources are clean up */
	protected void finalize() throws Throwable{
		try{
			close();
		} catch (Throwable e) {
			getLog().error(e.toString());
		}
		finally{
			super.finalize();
		}
	}
	
	/**
	 * Remove an entry from the database.  If the record doesn't exist nothing happens.
	 * @param key The entry to remove.
	 * @return the removed value, or null if optimize is true
	 */
	@Override
	public synchronized V remove(Object key){
		int which = shardFunction.pickShard(key);
		return(shards.get(which).remove(key));
	}
	

	/**
	 * Put an entry into the database
	 * @param key
	 * @param value
	 * @return The value previously associated with key, or null if optimize is true
	 */
	@Override
	public synchronized V put(K key,V value){
		int which = shardFunction.pickShard(key);
		return(shards.get(which).put(key, value));
	}
	

	/** Get an entry from the database
	 * 
	 * @param key
	 * @return the value in the database. null if there is no entry
	 */
	@Override
	public synchronized V get(Object key){
		int which = shardFunction.pickShard(key);
		return(shards.get(which).get(key));
	}
	


	/** Synchronously iterate over the entries in the database and call the appropriate methods in <param>iwClass</param>
	 * to do work.  See IteratorWorker for details on how the iteration works.
	 * @param iwClass the class to instantiate to do the work
	 * @param iwConfig any configuration parameters to pass to iwClass after it is instantiated during initialization
	 * @return the IteratorWorker after the work is complete.  
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public synchronized IteratorWorker<K,V> iterate(Class<? extends IteratorWorker<K, V>> iwClass, IteratorWorkerConfig iwConfig) throws InstantiationException, IllegalAccessException {
		
		IteratorWorker<K, V> ret = shards.get(0).iterate(iwClass, iwConfig);
		
		for(int i = 1; i < shards.size();i++){
			ret.combine(shards.get(i).iterate(iwClass, iwConfig));
		}
		
		return(ret);
	}
	
	/**
	 * @return the total number of records across all the databases.
	 */
	public synchronized Long sizeLong(){
		Long total = 0L;
		
		for(int i = 0; i < shards.size();i++){
			total += shards.get(i).sizeLong(); 
		}
		
		return(total);
	}
	
	

	/**
	 * Optimized clear operation.  This erases all records in the sharded databases
	 */
	public synchronized void clear(){
		for(int i = 0; i < shards.size();i++){
			shards.get(i).clear();
		}
	}

}
