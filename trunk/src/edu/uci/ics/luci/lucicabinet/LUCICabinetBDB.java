package edu.uci.ics.luci.lucicabinet;

import java.io.Serializable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import tokyocabinet.BDB;
import tokyocabinet.Util;

/**
	 * This is a class which creates a synchronized (thread-safe) key-value store backed by 
	 * a tokyo cabinet B-Tree Database (BDB).
	 * K is the key type
	 * V is the value type
*/
public class LUCICabinetBDB<K extends Serializable,V extends Serializable> extends LUCICabinetMap<K,V>{
	
	private BDB bdb = null;
	private ReentrantReadWriteLock rwlock = null;
	private boolean optimize = true;

	/** Open the database stored at the filePathName indicated.
	 *  If the file doesn't exist it will be created. 
	 * The file will be write locked while open by the file system. If the file is not closed the 
	 * underlying database will be damaged.
     *   If the database is "optimized" then put and removes will be non-blocking and will always return null.
     * This is a violation of the java Map contract, but cuts the database operations in half.
	 * 
	 * @param filePathAndName The name of the file to open, e.g."eraseme.tcb"
	 * @param optimize if true, then the database will always return null for put and remove operations
	 */
	public LUCICabinetBDB(String filePathAndName,boolean optimize){
		super();
		bdb = new BDB();
		rwlock = new ReentrantReadWriteLock(true);
		this.optimize = optimize;
		
		rwlock.writeLock().lock();
		try{
			if(bdb.open(filePathAndName,BDB.OWRITER | BDB.OCREAT)){
				return;
			}
			else{
				throw new RuntimeException("Error opening tokyo cabinet database, code:"+bdb.ecode()+":"+bdb.errmsg());
			}
		}
		finally{
			rwlock.writeLock().unlock();
		}
	}
	

	/**
    * Getter for the optimize setting of this database
	*/
	@Override
	public boolean getOptimize(){
		return optimize;
	}
	
    /**
    * Setter for the optimize setting of this database
	*/
	@Override
	public void setOptimize(boolean optimize){
		this.optimize = optimize;
	}
	
		
	
	
	/**
	 * Remove an entry from the database.  If the record doesn't exist nothing happens.
	 * @param key The entry to remove.
	 * @return the removed value, or null if optimize is true
	 */
	@Override
	public V remove(Object key){
		V ret = null;
		rwlock.writeLock().lock();
		try{
			if(!optimize){
				ret = get(key);
			}
			if(!bdb.out(Util.serialize(key))){
				if(bdb.ecode() != BDB.ENOREC){
					throw new RuntimeException("Error removing element from tokyo cabinet database, code:"+bdb.ecode());
				}
			}
		}
		finally{
			rwlock.writeLock().unlock();
		}
		return ret;
	}
	
	
	
	/**
	 * Put an entry into the database
	 * @param key
	 * @param value
	 * @return The value previously associated with key, or null if optimize is true
	 */
	@Override
	public V put(K key, V value){
		V ret = null;
		rwlock.writeLock().lock();
		try{
			if(!optimize){
				ret = get(key);
			}
			if (!bdb.put(Util.serialize(key),Util.serialize(value))){
				throw new RuntimeException("Error putting an element in tokyo cabinet database, code:"+bdb.ecode());
			}
		}
		finally{
			rwlock.writeLock().unlock();
		}
		return ret;
	}
	
	
	
	/** Get an entry from the database
	 * 
	 * @param key
	 * @return the value. null if there is no entry or the entry is null
	 */
	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key){
		rwlock.readLock().lock();
		try{
			byte[] value = bdb.get(Util.serialize(key));
			if(value != null){
				return (V) (Util.deserialize(value));
			}
			else{
				return null;
			}
		}
		finally{
			rwlock.readLock().unlock();
		}
	}
	
	
	
	
	/** Iterate over the entries in the database and call the appropriate methods in <param>iwClass</param>
	 * to do work.  See IteratorWorker for details on how the iteration works.
	 * @param iwClass the class to instantiate to do the work
	 * @param iwConfig any configuration parameters to pass to iwClass after it is instantiated during initialization
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	@SuppressWarnings("unchecked")
	public IteratorWorker<K,V> iterate(Class<? extends IteratorWorker<K,V>> iwClass,IteratorWorkerConfig iwConfig) throws InstantiationException, IllegalAccessException{
		
		IteratorWorker<K,V> iw = iwClass.newInstance();
		
		rwlock.writeLock().lock();
		try{
			iw.initialize(this,iwConfig);
		}
		finally{
			rwlock.readLock().lock();
			rwlock.writeLock().unlock();
		}
		
		try{
			boolean x = bdb.iterinit();
			boolean keepGoing = true;
			if(x){
				byte[] _key;
				while (keepGoing && ((_key = bdb.iternext()) != null)) {
					K key = (K) Util.deserialize(_key);
					V value = (V) Util.deserialize(bdb.get(_key));
					if(iw.iterate(key,value)){
						keepGoing = false;
					}
				}
			}
		}
		finally{
			rwlock.readLock().unlock();
		}
		
		rwlock.writeLock().lock();
		try{
			iw.shutdown(this);
		}
		finally{
			rwlock.writeLock().unlock();
		}
		return(iw);
	}
	
	
	
	
	/**
	 *  Close the database. This must be done to ensure database is not damaged on disk after being opened.
	 */
	public void close(){
		rwlock.writeLock().lock();
		try{
			if(bdb != null){
				if(!bdb.close()){
					throw new RuntimeException("Error closing a tokyo cabinet database, code:"+bdb.ecode()+":"+bdb.errmsg());
				}
				bdb = null;
			}
		}
		finally{
			rwlock.writeLock().unlock();
		}
	}
	
	
	
	/**
	 * Return the number of records in the database.
	 */
	public Long sizeLong(){
		rwlock.readLock().lock();
		try{
			return(bdb.rnum());
		}
		finally{
			rwlock.readLock().unlock();
		}
	}
	



	/**
	 * Optimized clear operation.  This erases all records in the database
	 */
	public void clear() {
		rwlock.writeLock().lock();
		try{
			bdb.vanish();
		}
		finally{
			rwlock.writeLock().unlock();
		}
	}

}