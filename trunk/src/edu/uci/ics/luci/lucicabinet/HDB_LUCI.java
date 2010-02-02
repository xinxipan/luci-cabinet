package edu.uci.ics.luci.lucicabinet;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import tokyocabinet.HDB;

/**
	 * This is a class which creates a synchronized (thread-safe) key-value store backed by 
	 * a tokyo cabinet Hash Database (HDB).
	 *
	 * <pre>
	 *  HDB_LUCI hdb_luci = new HDB_LUCI();
	 *  </pre>
*/
public class HDB_LUCI extends DB_LUCI{
	
	private HDB hdb = null;
	ReentrantReadWriteLock rwlock = null;

	public HDB_LUCI() {
		super();
		hdb = new HDB();
		rwlock = new ReentrantReadWriteLock(true);
	}
	
	/** Open the database stored at the filePathName indicated.
	 *  If the file doesn't exist it will be created. 
	 * The file will be write locked while open by the file system. If the file is not closed the 
	 * underlying database will be damaged.
	 * 
	 * @param filePathAndName The name of the file to open.
	 */
	public void open(String filePathAndName){
		rwlock.writeLock().lock();
		try{
			if(hdb.open(filePathAndName,HDB.OWRITER | HDB.OCREAT)){
				return;
			}
			else{
				throw new RuntimeException("Error opening tokyo cabinet database, code:"+hdb.ecode());
			}
		}
		finally{
			rwlock.writeLock().unlock();
		}
	}
	
	/**
	 * Remove an entry from the database. If the key doesn't exist HDB.ENOREC will be returned.
	 * @param key The entry to remove.
	 */
	public void remove(byte[] key){
		rwlock.writeLock().lock();
		try{
			if(hdb.out(key)){
				return;
			}
			else{
				throw new RuntimeException("Error removing element from tokyo cabinet database, code:"+hdb.ecode());
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
	public void put(byte[] key, byte[] value){
		rwlock.writeLock().lock();
		try{
			if (hdb.put(key,value)){
				return;
			}
			else{
				throw new RuntimeException("Error putting an element in tokyo cabinet database, code:"+hdb.ecode());
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
	public byte[] get(byte[] key){
		rwlock.readLock().lock();
		try{
			return(hdb.get(key));
		}
		finally{
			rwlock.readLock().unlock();
		}
	}
	
	/** Iterate over the entries in the database and call the appropriate methods in <param>iw</param>
	 * to do work.  See IteratorWorker for details on how the iteration works.
	 * @param iw
	 */
	public void iterate(IteratorWorker iw){
		rwlock.writeLock().lock();
		try{
			iw.initialize(this);
		}
		finally{
			rwlock.readLock().lock();
			rwlock.writeLock().unlock();
		}
		
		try{
			boolean x = hdb.iterinit();
			if(x){
				byte[] key;
				while ((key = hdb.iternext()) != null) {
					iw.iterate(key,hdb.get(key));
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
	}
	
	/** Close the database. This must be done to ensure database is not damaged on disk after being opened.
	 */
	public void close(){
		rwlock.writeLock().lock();
		try{
			if(hdb.close()){
				return;
			}
			else{
				throw new RuntimeException("Error closing a tokyo cabinet database, code:"+hdb.ecode());
			}
		}
		finally{
			rwlock.writeLock().unlock();
		}
	}
	
	public Long size(){
		/* Write lock to make sure that everything finishes */
		rwlock.writeLock().lock();
		try{
			return(hdb.rnum());
		}
		finally{
			rwlock.writeLock().unlock();
		}
	}
	
}