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
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1435393836919236897L;
	
	private BDB bdb = null;
	private ReentrantReadWriteLock rwlock = null;

	/** Open the database stored at the filePathName indicated.
	 *  If the file doesn't exist it will be created. 
	 * The file will be write locked while open by the file system. If the file is not closed the 
	 * underlying database will be damaged.
	 * 
	 * @param filePathAndName The name of the file to open, e.g."eraseme.tch"
	 */
	public LUCICabinetBDB(String filePathAndName){
		super();
		bdb = new BDB();
		rwlock = new ReentrantReadWriteLock(true);
		
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
	 * Remove an entry from the database.  If the record doesn't exist nothing happens.
	 * @param key The entry to remove.
	 * @return the removed value
	 */
	@Override
	public V remove(Object key){
		V ret = null;
		rwlock.writeLock().lock();
		try{
			ret = get(key);
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
	 * @return The value previously associated with key, or null
	 */
	@Override
	public V put(K key, V value){
		V ret = null;
		rwlock.writeLock().lock();
		try{
			ret = get(key);
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
	 * @return the value. null if there is no entry
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
	
	
	
	
	/** Iterate over the entries in the database and call the appropriate methods in <param>iw</param>
	 * to do work.  See IteratorWorker for details on how the iteration works.
	 * @param iw
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

}