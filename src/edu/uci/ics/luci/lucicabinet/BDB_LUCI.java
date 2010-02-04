package edu.uci.ics.luci.lucicabinet;

import java.io.Serializable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import tokyocabinet.BDB;
import tokyocabinet.Util;

/**
	 * This is a class which creates a synchronized (thread-safe) key-value store backed by 
	 * a tokyo cabinet B-Tree Database (BDB). 
*/
public class BDB_LUCI extends DB_LUCI{
	
	private BDB bdb = null;
	private ReentrantReadWriteLock rwlock = null;

	/** Open the database stored at the filePathName indicated.
	 *  If the file doesn't exist it will be created. 
	 * The file will be write locked while open by the file system. If the file is not closed the 
	 * underlying database will be damaged.
	 * 
	 * @param filePathAndName The name of the file to open, e.g."eraseme.tch"
	 */
	public BDB_LUCI(String filePathAndName){
		super();
		bdb = new BDB();
		rwlock = new ReentrantReadWriteLock(true);
		rwlock.writeLock().lock();
		try{
			if(bdb.open(filePathAndName,BDB.OWRITER | BDB.OCREAT)){
				return;
			}
			else{
				throw new RuntimeException("Error opening tokyo cabinet database, code:"+bdb.ecode());
			}
		}
		finally{
			rwlock.writeLock().unlock();
		}
	}
	
	/**
	 * Remove an entry from the database.  If the record doesn't exist nothing happens.
	 * @param key The entry to remove.
	 */
	public void remove(Serializable key){
		rwlock.writeLock().lock();
		try{
			if(bdb.out(Util.serialize(key))){
				return;
			}
			else{
				if(bdb.ecode() != BDB.ENOREC){
					throw new RuntimeException("Error removing element from tokyo cabinet database, code:"+bdb.ecode());
				}
			}
		}
		finally{
			rwlock.writeLock().unlock();
		}
	}
	
	/**
	 * Put an entry into the database
	 * @param key
	 * @param value
	 */
	public void put(Serializable key, Serializable value){
		rwlock.writeLock().lock();
		try{
			if (bdb.put(Util.serialize(key),Util.serialize(value))){
				return;
			}
			else{
				throw new RuntimeException("Error putting an element in tokyo cabinet database, code:"+bdb.ecode());
			}
		}
		finally{
			rwlock.writeLock().unlock();
		}
	}
	
	/** Get an entry from the database
	 * 
	 * @param key
	 * @return the value. null if there is no entry
	 */
	public Serializable get(Serializable key){
		rwlock.readLock().lock();
		try{
			byte[] value = bdb.get(Util.serialize(key));
			if(value != null){
				return (Serializable) (Util.deserialize(value));
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
	 */
	public IteratorWorker iterate(IteratorWorker iw){
		rwlock.writeLock().lock();
		try{
			iw.initialize(this);
		}
		finally{
			rwlock.readLock().lock();
			rwlock.writeLock().unlock();
		}
		
		try{
			boolean x = bdb.iterinit();
			if(x){
				byte[] _key;
				while ((_key = bdb.iternext()) != null) {
					Serializable key = (Serializable) Util.deserialize(_key);
					Serializable value = (Serializable) Util.deserialize(bdb.get(_key));
					iw.iterate(key,value);
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
	
	
	/** Close the database. This must be done to ensure database is not damaged on disk after being opened.
	 */
	public void close(){
		rwlock.writeLock().lock();
		try{
			if(bdb.close()){
				return;
			}
			else{
				throw new RuntimeException("Error closing a tokyo cabinet database, code:"+bdb.ecode()+":"+bdb.errmsg());
			}
		}
		finally{
			rwlock.writeLock().unlock();
		}
	}
	
	/**
	 * Return the number of records in the database.
	 */
	public Long size(){
		rwlock.readLock().lock();
		try{
			return(bdb.rnum());
		}
		finally{
			rwlock.readLock().unlock();
		}
	}
	
}