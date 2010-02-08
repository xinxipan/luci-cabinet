package edu.uci.ics.luci.lucicabinet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.uci.ics.luci.lucicabinet.library.ShardFunctionSimple;

/**
 * This class implements a sharded database. This means that key-value pairs are stored in a collection of different databases
 * based on a function of their key.  A sharded database speeds up concurrent access and enables scaling to
 * large databases (in theory).
 * 
 * Although the methods are synchronized to manage concurrent access to this object, the actual work of accessing the
 * database is offloaded to a separate worker queue per shard so that methods should return quickly.
 * 
 */
public class LUCICabinetMap_Shard<K extends Serializable,V extends Serializable> extends LUCICabinetMap<K,V>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1403683707933184146L;
	
	private List<LUCICabinetMap<K,V>> shards = null;
	private List<ExecutorService> threadExecutors = null;
	private ShardFunction shardFunction;
	
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
	 * (effectively iterating more than once across entries) 
	 * The sharding function is the default based on a modulo of the hash function.
	 */
	public LUCICabinetMap_Shard(List<LUCICabinetMap<K,V>> shards) {
		this(shards,new ShardFunctionSimple(shards.size()));
	}
	
	/**
	 * 
	 * @param shard A list of databases to shard across.  If any of these databases are the same database, or are
	 * remote wrappers for the same database, then iterations will go across each copy of the database one time
	 * (effectively iterating more than once across entries) 
	 * @param sf An object which defines how to associate keys with shards.
	 */
	public LUCICabinetMap_Shard(List<LUCICabinetMap<K,V>> shard,ShardFunction sf) {
		super();
		
		this.shards = shard;
		this.shardFunction = sf;
		
		threadExecutors = new ArrayList<ExecutorService>(shard.size());
		for(int i = 0; i < shard.size();i++){
			threadExecutors.add(Executors.newSingleThreadExecutor());
		}
	}
	
	
	
	/**
	 * A wrapper for shutdown
	 */
	@Override
	public synchronized void close() {
		shutdown();
	}
	
	/**
	 * Gives all shards at least 2 minutes to complete while preventing new jobs from being executed.
	 * After fair warning all shards are closed.
	 */
	protected synchronized void shutdown() {

		/* Tell the current commands finish */
		if(threadExecutors != null){
			for(ExecutorService te:threadExecutors){
				if (te != null) {
					te.shutdown();
				}
			}
		
			/* Give time to shut down */
			for(ExecutorService te:threadExecutors){
				try {
					if (!te.awaitTermination(120, TimeUnit.SECONDS)) {
						te.shutdownNow();
						if (!te.awaitTermination(120, TimeUnit.SECONDS)) {
							getLog().log(Level.ERROR, "Thread pool did not terminate");
						}
					}
				} catch (InterruptedException e) {
					te.shutdownNow();
				}
			}
			threadExecutors.clear();
			threadExecutors = null;
		}
		
		/* Close the shards */ 
		if(shards != null){
			for(LUCICabinetMap<K,V> db:shards){
				db.close();
			}
		
			shards.clear();
			shards = null;
		}
	}
	

	/** Wrapper for shutdown to make sure all resources are clean up */
	protected void finalize() throws Throwable{
		try{
			shutdown();
		} catch (Throwable e) {
			getLog().error(e.toString());
		}
		finally{
			super.finalize();
		}
	}
	
	private static class RemoveWrapper<K extends Serializable,V extends Serializable> implements Runnable{
		private Object key;
		public V result;
		private LUCICabinetMap<K,V> db;

		public RemoveWrapper(LUCICabinetMap<K,V> db,Object key){
			this.db = db;
			this.key = key;
		}
		
		public void run() {
			result = (V) db.remove(key);
		};
	}
	
	/**
	 * An asynchronous remove command. Runs on a separate thread.
	 * @param key the entry to remove
	 */
	public synchronized void removeAsync(Object key) {
		int which = shardFunction.pickShard(key);
		threadExecutors.get(which).execute(new RemoveWrapper<K,V>(shards.get(which),key));
	}
	
	/**
	 * An synchronous remove command. Runs on a separate thread, blocks until it is done.
	 * @param key the entry to remove
	 * @return 
	 */
	public synchronized V removeSync(Object key) {
		int which = shardFunction.pickShard(key);
		RemoveWrapper<K,V> iw = new RemoveWrapper<K,V>(shards.get(which),key);
		Future<?> f = threadExecutors.get(which).submit(iw);
		try {
			f.get();
		} catch (InterruptedException e) {
			getLog().log(Level.ERROR, "Interrupted while waiting for remove to complete",e);
		} catch (ExecutionException e) {
			getLog().log(Level.ERROR, "Remove failed",e);
		}
		return iw.result;
	}
	
	/**
	 * Remove an entry from the database asynchronously. 
	 * @param key The entry to remove.
	 * @return the return value is always null.  If you actually want the value removed, use removeSync
	 */
	public synchronized V remove(Object key){
		removeAsync(key);
		return null;
	}
	

	private static class PutWrapper<K extends Serializable,V extends Serializable> implements Runnable{
		private LUCICabinetMap<K,V> db;
		private K key;
		private V value;
		public V result;

		public PutWrapper(LUCICabinetMap<K,V> db,K key,V value){
			this.db = db;
			this.key = key;
			this.value = value;
		}
		
		public void run() {
			result = db.put(key, value);
		}
	}
	
	
	/**
	 * An asynchronous put command. Runs on a separate thread.
	 * 
	 * @param key
	 * @param value
	 */
	public synchronized void putAsync(K key,V value) {
		int which = shardFunction.pickShard(key);
		threadExecutors.get(which).execute(new PutWrapper<K,V>(shards.get(which),key,value));
	}
	
	/**
	 * A synchronous put command. Runs on a separate thread, blocks until it is done.
	 * @param key
	 * @param value
	 * @return 
	 */
	public synchronized V putSync(K key, V value){
		int which = shardFunction.pickShard(key);
		PutWrapper<K,V> pw = new PutWrapper<K,V>(shards.get(which),key,value);
		Future<?> f = threadExecutors.get(which).submit(pw);
		try {
			f.get();
		} catch (InterruptedException e) {
			getLog().log(Level.ERROR, "Interrupted while waiting for put to complete",e);
		} catch (ExecutionException e) {
			getLog().log(Level.ERROR, "Put failed",e);
		}
		return(pw.result);
	}
	

	/**
	 * Put a key value pair in the database asynchronously 
	 * @param key
	 * @param value
	 * @return always returns null.  If you want the value that is overwritten as per the Map interface, try using putAsync instead.
	 */
	public synchronized V put(K key,V value){
		putAsync(key,value);
		return(null);
	}
	

	private static class GetWrapper<K extends Serializable,V extends Serializable> implements Runnable{
		private LUCICabinetMap<K,V> db;
		private Object key;
		public V result = null;

		public GetWrapper(LUCICabinetMap<K,V> db, Object key){
			this.db = db;
			this.key = key;
		}
		
		public void run() {
			result = db.get(key);
		};
	}
	

	/**
	 * An asynchronous get command. Runs on a separate thread. Not sure
	 * why anyone would want this.
	 * @param key the entry to get
	 */
	public synchronized void getAsync(K key) {
		int which = shardFunction.pickShard(key);
		threadExecutors.get(which).execute(new GetWrapper<K,V>(shards.get(which),key));
	}
	
	/**
	 * An synchronous get command. Runs on a separate thread, blocks until it is done.
	 * @param key the entry to get
	 */
	public synchronized V getSync(Object key) {
		int which = shardFunction.pickShard(key);
		GetWrapper<K,V> g = new GetWrapper<K,V>(shards.get(which),key);
		Future<?> f = threadExecutors.get(which).submit(g);
		try {
			f.get();
		} catch (InterruptedException e) {
			getLog().log(Level.ERROR, "Interrupted while waiting for remove to complete",e);
		} catch (ExecutionException e) {
			getLog().log(Level.ERROR, "Remove failed",e);
		}
		return(g.result);
	}
	

	/** Get an entry from the database
	 * 
	 * @param key
	 * @return the value in the database. null if there is no entry
	 */
	public synchronized V get(Object key){
		return(getSync(key));
	}
	

	private static class IterateWrapper<K extends Serializable,V extends Serializable> implements Runnable{
		IteratorWorker<K,V> result = null;
		private Class<? extends IteratorWorker<K, V>> iw = null;
		private IteratorWorkerConfig iwc = null;
		private LUCICabinetMap<K, V> db = null;

		public IterateWrapper(LUCICabinetMap<K,V> db, Class<? extends IteratorWorker<K, V>> iw, IteratorWorkerConfig iwc){
			this.db = db;
			this.iw = iw;
			this.iwc = iwc;
		}
		
		public void run() {
			try {
				result = db.iterate(iw,iwc);
			} catch (InstantiationException e) {
				getLog().log(Level.ERROR, "Iterate failed",e);
			} catch (IllegalAccessException e) {
				getLog().log(Level.ERROR, "Iterate failed",e);
			}
		};
	}	
	

	/**
	 * Asynchronously run a job that iterates through all the entries in the database.
	 * @param iw a class representing the work to be done.
	 */
	public synchronized void iterateASync(Class<? extends IteratorWorker<K, V>> iw, IteratorWorkerConfig iwc) throws InstantiationException, IllegalAccessException {
		for(int i = 0; i < shards.size();i++){
			threadExecutors.get(i).execute(new IterateWrapper<K,V>(shards.get(i),iw,iwc));
		}
	}

	/**
	 * Synchronously run a job that iterates through all the entries in the database. Since the IteratorWorker
	 * may have internal state that the caller wants to access after the iteration, the returned value is the 
	 * IteratorWorker after iterations.   
	 * @param iw a class representing the work to be done.
	 * @return the IteratorWorker after the work is complete.  
	 */
	@SuppressWarnings("unchecked")
	public synchronized IteratorWorker<K,V> iterateSync(Class<? extends IteratorWorker<K, V>> iw, IteratorWorkerConfig iwc) throws InstantiationException, IllegalAccessException {
		Future<?>[] futures = new Future<?>[shards.size()];
		IterateWrapper<K,V>[] wrappers = new IterateWrapper[shards.size()];
		
		for(int i = 0; i < shards.size();i++){
			wrappers[i] = new IterateWrapper<K,V>(shards.get(i),iw,iwc);
			futures[i] = threadExecutors.get(i).submit(wrappers[i]);
		}
		
		IteratorWorker<K,V> ret = null;
		try {
			futures[0].get();
			ret = wrappers[0].result;
			for(int i = 1; i < shards.size();i++){
				futures[i].get();
				ret.combine(wrappers[i].result);
			}
		} catch (InterruptedException e) {
			getLog().log(Level.ERROR, "Interrupted while waiting for iterate to complete",e);
		} catch (ExecutionException e) {
			getLog().log(Level.ERROR, "Iterate failed",e);
		}
		return(ret);
	}
	
	
	/** Synchronously iterate over the entries in the database and call the appropriate methods in <param>iw</param>
	 * to do work.  See IteratorWorker for details on how the iteration works.  If there no information needs to be returned or
	 * collected, consider using <pre>iterateAsync</pre>.
	 * @param iw
	 */
	@Override
	public IteratorWorker<K, V> iterate(Class<? extends IteratorWorker<K, V>> iw, IteratorWorkerConfig iwc) throws InstantiationException, IllegalAccessException {
			return(iterateSync(iw,iwc));
	}
	
	
	private static class SizeWrapper<K extends Serializable,V extends Serializable> implements Runnable{
		public Long result = null;
		private LUCICabinetMap<K,V> db;
		
		SizeWrapper(LUCICabinetMap<K,V> db){
			this.db = db;
		}

		public void run() {
			result = db.sizeLong();
		};
	}
	
	/**
	 * The number of entries in the database.
	 */
	@SuppressWarnings("unchecked")
	public synchronized Long sizeLong(){
		SizeWrapper<K,V>[] wrappers = new SizeWrapper[shards.size()];
		Future<?>[] futures = new Future<?>[shards.size()];
		Long total = 0L;
		
		for(int i = 0; i < shards.size();i++){
			wrappers[i] = new SizeWrapper<K,V>(shards.get(i));
			futures[i] = threadExecutors.get(i).submit(wrappers[i]);
		}
		
		for(int i = 0; i < shards.size();i++){
			try {
				futures[i].get();
				total += wrappers[i].result;
			} catch (InterruptedException e) {
				getLog().log(Level.ERROR, "Interrupted while waiting for iterate to complete",e);
			} catch (ExecutionException e) {
				getLog().log(Level.ERROR, "Iterate failed",e);
			}
		}
		return(total);
	}

}
