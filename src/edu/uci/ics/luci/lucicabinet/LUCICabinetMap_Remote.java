package edu.uci.ics.luci.lucicabinet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.uci.ics.luci.lucicabinet.LUCI_Butler.ServerResponse;

/**
 * This class enables synchronized and asynchronous access to a tokyo-cabinet database over a socket connection.
 * The expected use is that the remote end would be running the LUCI_Butler class and that this class would connect to it.
 * Jobs are guaranteed to be executed in the order submitted, but the underlying remote database may be is concurrent,
 * so other clients could change it in unexpected ways. 
 * 
 * Although the methods themselves are synchronized to manage concurrent access to this object, the actual work of accessing the
 * database is offloaded to a worker queue so that methods should return quickly. To support this, when this class is "optimized", 
 * the put and remove interfaces behave differently than expected for Map as they return. If not "optimized" then they return
 * the overwritten or removed value.
 */
public class LUCICabinetMap_Remote<K extends Serializable,V extends Serializable> extends LUCICabinetMap<K,V>{

	protected transient ExecutorService threadExecutor = null;
	protected transient Socket clientSocket = null;
	protected transient ObjectOutputStream oos = null;
	protected transient ObjectInputStream ois = null;

	protected boolean optimize = true;
	
	private static transient volatile Logger log = null;
	public static Logger getLog(){
		if(log == null){
			log = Logger.getLogger(LUCICabinetMap_Remote.class);
		}
		return log;
	}
	
	
	/**
	 * This method opens the connection to the remote LUCI_Butler service. 
	 *  
	 * <p>
	 * If the database is "optimized" then put and removes will be non-blocking and will always return null.
     * This is a violation of the java Map contract, but cuts the database operations in half.
	
	 * @param host The remote host to connect to, e.g. "localhost", "192.128.1.20"
	 * @param port The port that the remote host is listening on.
	 * @param optimize if true, then the database will always return null for put and remove operations
	 */
	public LUCICabinetMap_Remote(String host,Integer port,boolean optimize) {
		super();
		this.optimize = optimize;
		
		threadExecutor = Executors.newSingleThreadExecutor();			
		
		try{
			clientSocket = new Socket(host,port);
		
			oos = new ObjectOutputStream(clientSocket.getOutputStream());
		
			ois = new ObjectInputStream(clientSocket.getInputStream());
		
			ServerResponse okay;
			try {
				okay = (ServerResponse) ois.readObject();
				if(optimize && !okay.equals(ServerResponse.CONNECTION_OKAY_OPTIMIZE)){
					throw new RuntimeException("Remote host did not send a connection okay signal");
				}
				if(!optimize && !okay.equals(ServerResponse.CONNECTION_OKAY_UNOPTIMIZE)){
					throw new RuntimeException("Remote host did not send a connection okay signal");
				}
			} catch (ClassNotFoundException e) {
				throw new IOException("Remote host did not send a connection okay signal"+e);
			} 
		} catch (UnknownHostException e) {
			getLog().log(Level.ERROR, "Unable to open "+host+":"+port+" for a connection",e);
		} catch (IOException e) {
			getLog().log(Level.ERROR, "Unable to open connection",e);
		}
	}
	
	
	/**
	 * Getter for the optimize setting of this database
	 */
	@Override
	public synchronized boolean getOptimize(){
		return optimize;
	}

	private class OptimizeWrapper implements Runnable{
		
		private Boolean optimize;

		OptimizeWrapper(boolean optimize){
			this.optimize = optimize;
		}

		public void run() {

			try{
				oos.writeObject(LUCI_Butler.ServerCommands.SET_OPTIMIZE);
			} catch (IOException e) {
				getLog().log(Level.ERROR, "Unable to write "+LUCI_Butler.ServerCommands.CLEAR+" command",e);
			}
			
			try{
				oos.writeObject(optimize);
			} catch (IOException e) {
				getLog().log(Level.ERROR, "Unable to write "+LUCI_Butler.ServerCommands.CLEAR+" command",e);
			}
			
			checkForError(ois);
			setOptimize(optimize);
		};
	}
	
	/**
	 * Set optimization for remote database. This operations blocks until all previous operations are complete.
	 */
	@Override
	public synchronized void setOptimize(boolean optimize){
		
		OptimizeWrapper wrapper = new OptimizeWrapper(optimize);
		Future<?> f = threadExecutor.submit(wrapper);
		try {
			f.get();
		} catch (InterruptedException e) {
			getLog().log(Level.ERROR, "Interrupted while waiting for optimize to complete",e);
		} catch (ExecutionException e) {
			getLog().log(Level.ERROR, "Optimize failed",e);
		}
	}
	
	
	/**
	 * Gives all asynchronous operations up to 2 minutes to complete then tells
	 * the remote server that this connection is shutting down, then cleans up
	 * all the resources created by this class and closes the remote database.
	 */
	@Override
	public synchronized void close() {

		/* Let the current commands finish */
		if (threadExecutor != null) {
			threadExecutor.shutdown();
			try {
				if (!threadExecutor.awaitTermination(120, TimeUnit.SECONDS)) {
					threadExecutor.shutdownNow();
					if (!threadExecutor.awaitTermination(120, TimeUnit.SECONDS)) {
						getLog().log(Level.ERROR, "Thread pool did not terminate");
					}
				}
			} catch (InterruptedException e) {
				threadExecutor.shutdownNow();
			}
			threadExecutor = null;
		}

		/* Close the remote database */
		if (oos != null) {
			try {
				oos.writeObject(LUCI_Butler.ServerCommands.CLOSE);
			} catch (IOException e) {
				getLog().log( Level.ERROR, "Unable to write " + LUCI_Butler.ServerCommands.CLOSE + " command", e);
			}
			checkForError(ois);
		}

		/* close the connections */
		try {
			if (ois != null) {
				ois.close();
				ois = null;
			}
		} catch (IOException e) {
		}

		try {
			if (oos != null) {
				oos.close();
				oos = null;
			}
		} catch (IOException e) {
		}

		try {
			if (clientSocket != null) {
				clientSocket.close();
				clientSocket = null;
			}
		} catch (IOException e) {
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
	
	
	
	
	private class RemoveWrapper implements Runnable{
		private Object key;
		public V result = null;

		public RemoveWrapper(Object key){
			this.key = key;
		}
		
		@SuppressWarnings("unchecked")
		public void run() {
			try {
				oos.writeObject(LUCI_Butler.ServerCommands.REMOVE);
			} catch (IOException e) {
				getLog().log(Level.ERROR, "Unable to write "+LUCI_Butler.ServerCommands.REMOVE+" command",e);
			}
		
			try {
				oos.writeObject(key);
				oos.flush();
			} catch (IOException e) {
				getLog().log(Level.ERROR, "Unable to write "+LUCI_Butler.ServerCommands.REMOVE+" command parameter",e);
			}
			
			if(!optimize){
				try {
					result = (V) ois.readObject();
				} catch (IOException e) {
					getLog().log(Level.ERROR, "Unable to read a result from object input stream",e);
				} catch (ClassNotFoundException e) {
					getLog().log(Level.ERROR, "Unable to read a result from object input stream",e);
				}
			}
				
			checkForError(ois);
		};
	}
	
	
	
	/**
	 * An asynchronous remove command. Runs on a separate thread.
	 * @param key the entry to remove
	 */
	public synchronized void removeAsync(Object key) {
		threadExecutor.execute(new RemoveWrapper(key));
	}
	
	/**
	 * An synchronous remove command. Runs on a separate thread, blocks until it is done.
	 * @param key the entry to remove
	 * @return the value that was removed
	 */
	public synchronized V removeSync(Object key) {
		RemoveWrapper iw = new RemoveWrapper(key);
		Future<?> f = threadExecutor.submit(iw);
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
	 * Remove an entry from the database.  If the record doesn't exist nothing happens. This operation is blocking if
	 * optimize is set to false.
	 * @param key The entry to remove.
	 * @return the removed value, or null if optimize is true
	 */
	public synchronized V remove(Object key){
		if(optimize){
			removeAsync(key);
			return null;
		}
		else{
			return(removeSync(key));
		}
	}
	

	private class PutWrapper implements Runnable{
		private K key;
		private V value;
		public V result = null;

		public PutWrapper(K key,V value){
			this.key = key;
			this.value = value;
		}
		
		@SuppressWarnings("unchecked")
		public void run() {
			
			try {
				oos.writeObject(LUCI_Butler.ServerCommands.PUT);
			} catch (IOException e) {
				getLog().log(Level.ERROR, "Unable to write "+LUCI_Butler.ServerCommands.PUT+" command",e);
			}
		
			try {
				oos.writeObject(key);
			} catch (IOException e) {
				getLog().log(Level.ERROR, "Unable to write "+LUCI_Butler.ServerCommands.PUT+" command parameter,key",e);
			}
		
			try {
				oos.writeObject(value);
				oos.flush();
			} catch (IOException e) {
				getLog().log(Level.ERROR, "Unable to write "+LUCI_Butler.ServerCommands.PUT+" command parameter,value",e);
			}
			
			if(!optimize){
				try {
					result = (V) ois.readObject();
				} catch (IOException e) {
					getLog().log(Level.ERROR, "Unable to read a result from object input stream",e);
				} catch (ClassNotFoundException e) {
					getLog().log(Level.ERROR, "Unable to read a result from object input stream",e);
				}
			}
		
			checkForError(ois);
		}
	}
	
	
	/**
	 * An asynchronous put command. Runs on a separate thread.
	 * 
	 * @param key
	 * @param value
	 */
	public synchronized void putAsync(K key,V value) {
		threadExecutor.execute(new PutWrapper(key,value));
	}
	
	/**
	 * A synchronous put command. Runs on a separate thread, blocks until it is done.
	 * @param key
	 * @param value 
	 * @return the overwritten value or null.
	 */
	public synchronized V putSync(K key, V value){
		PutWrapper pw = new PutWrapper(key,value);
		Future<?> f = threadExecutor.submit(pw);
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
	 * Put a key value pair in the database. If optimize is true, this is a non-blocking method. 
	 * @param key
	 * @param value
	 * @return the overwritten value, or null if optimize is true
	 */
	public synchronized V put(K key,V value){
		if(optimize){
			putAsync(key,value);
			return(null);
		}
		else{
			return(putSync(key,value));
		}
	}
	

	private class GetWrapper implements Runnable{
		private Object key;
		public V result = null;

		public GetWrapper(Object key){
			this.key = key;
		}
		
		@SuppressWarnings("unchecked")
		public void run() {
			try{
				oos.writeObject(LUCI_Butler.ServerCommands.GET);
			} catch (IOException e) {
				getLog().log(Level.ERROR, "Unable to write "+LUCI_Butler.ServerCommands.GET+" command",e);
			}
			
			try {
				oos.writeObject(key);
				oos.flush();
			} catch (IOException e) {
				getLog().log(Level.ERROR, "Unable to write "+LUCI_Butler.ServerCommands.GET+" command parameter, key",e);
			}
			
			try {
				result = (V) ois.readObject();
			} catch (IOException e) {
				getLog().log(Level.ERROR, "Unable to read a result from object input stream",e);
			} catch (ClassNotFoundException e) {
				getLog().log(Level.ERROR, "Unable to read a result from object input stream",e);
			}
			
			checkForError(ois);
				
		};
	}
	

	/**
	 * An asynchronous get command. Runs on a separate thread. Not sure
	 * why anyone would want this.
	 * @param key the entry to get
	 */
	public synchronized void getAsync(K key) {
		threadExecutor.execute(new GetWrapper(key));
	}
	
	/**
	 * An synchronous get command. Runs on a separate thread, blocks until it is done.
	 * @param key the entry to get
	 */
	public synchronized V getSync(Object key) {
		GetWrapper g = new GetWrapper(key);
		Future<?> f = threadExecutor.submit(g);
		try {
			f.get();
		} catch (InterruptedException e) {
			getLog().log(Level.ERROR, "Interrupted while waiting for get to complete",e);
		} catch (ExecutionException e) {
			getLog().log(Level.ERROR, "Get failed",e);
		}
		return(g.result);
	}
	

	/** Get an entry from the database. This is a blocking method which forces all previous operations to be
	 * completed before returning a value.
	 * 
	 * @param key
	 * @return the value in the database. null if there is no entry
	 */
	public synchronized V get(Object key){
		return(getSync(key));
	}
	

	private class IterateWrapper implements Runnable{
		IteratorWorker<K,V> result = null;
		private Class<? extends IteratorWorker<K, V>> iw = null;
		private IteratorWorkerConfig iwc = null;

		public IterateWrapper(Class<? extends IteratorWorker<K, V>> iw, IteratorWorkerConfig iwc){
			this.iw = iw;
			this.iwc = iwc;
		}
		
		@SuppressWarnings("unchecked")
		public void run() {
			try {
				oos.writeObject(LUCI_Butler.ServerCommands.ITERATE);
			} catch (IOException e) {
				getLog().log(Level.ERROR, "Unable to write "+LUCI_Butler.ServerCommands.ITERATE+" command",e);
			}
				
			try {
				oos.writeObject(this.iw);
			} catch (IOException e) {
				getLog().log(Level.ERROR, "Unable to write "+LUCI_Butler.ServerCommands.ITERATE+" parameter, iw",e);
			}
			
			try {
				oos.writeObject(this.iwc);
				oos.flush();
			} catch (IOException e) {
				getLog().log(Level.ERROR, "Unable to write "+LUCI_Butler.ServerCommands.ITERATE+" parameter, iwc",e);
			}

			try {
				result = (IteratorWorker<K,V>) ois.readObject();
			} catch (IOException e) {
				getLog().log(Level.ERROR, "Unable to read a result from object input stream",e);
			} catch (ClassNotFoundException e) {
				getLog().log(Level.ERROR, "Unable to read a result from object input stream",e);
			}
				
			checkForError(ois);
		};
	}	
	

	/** Iterate over the entries in the database and call the appropriate methods in <param>iwClass</param>
	 * to do work.  See IteratorWorker for details on how the iteration works. This is a non-blocking method.
	 * @param iwClass the class to instantiate to do the work
	 * @param iwConfig any configuration parameters to pass to iwClass after it is instantiated during initialization
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public synchronized void iterateASync(Class<? extends IteratorWorker<K, V>> iwClass, IteratorWorkerConfig iwConfig) throws InstantiationException, IllegalAccessException {
		threadExecutor.execute(new IterateWrapper(iwClass,iwConfig));
	}


	/** Iterate over the entries in the database and call the appropriate methods in <param>iwClass</param>
	 * to do work.  See IteratorWorker for details on how the iteration works. This is a blocking method.
	 * @param iwClass the class to instantiate to do the work
	 * @param iwConfig any configuration parameters to pass to iwClass after it is instantiated during initialization
	 * @return the IteratorWorker after the work is complete.  
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public synchronized IteratorWorker<K, V> iterateSync(Class<? extends IteratorWorker<K, V>> iwClass, IteratorWorkerConfig iwConfig) throws InstantiationException, IllegalAccessException {
		IterateWrapper wrapper = new IterateWrapper(iwClass,iwConfig);
		Future<?> f = threadExecutor.submit(wrapper);
		try {
			f.get();
		} catch (InterruptedException e) {
			getLog().log(Level.ERROR, "Interrupted while waiting for iterate to complete",e);
		} catch (ExecutionException e) {
			getLog().log(Level.ERROR, "Iterate failed",e);
		}
		return(wrapper.result);
	}
	
	
	/** Synchronously iterate over the entries in the database and call the appropriate methods in <param>iwClass</param>
	 * to do work.  See IteratorWorker for details on how the iteration works.  If no information needs to be returned or
	 * collected, consider using <pre>iterateAsync</pre>.
	 * @param iwClass the class to instantiate to do the work
	 * @param iwConfig any configuration parameters to pass to iwClass after it is instantiated during initialization
	 * @return the IteratorWorker after the work is complete.  
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	@Override
	public IteratorWorker<K, V> iterate(Class<? extends IteratorWorker<K, V>> iwClass, IteratorWorkerConfig iwConfig) throws InstantiationException, IllegalAccessException {
		return(iterateSync(iwClass,iwConfig));
	}
	
	
	private class SizeWrapper implements Runnable{
		Long result = null;

		public void run() {

			try{
				oos.writeObject(LUCI_Butler.ServerCommands.SIZE);
				oos.flush();
			} catch (IOException e) {
				getLog().log(Level.ERROR, "Unable to write "+LUCI_Butler.ServerCommands.SIZE+" command",e);
			}
			
			try {
				result = (Long) ois.readObject();
			} catch (IOException e) {
				getLog().log(Level.ERROR, "Unable to read a result from object input stream",e);
			} catch (ClassNotFoundException e) {
				getLog().log(Level.ERROR, "Unable to read a result from object input stream",e);
			}
			
			checkForError(ois);
				
		};
	}
	
	/**
	 * Get the number of entries in the database.
	 * @return the number of entries in the database.
	 */
	public synchronized Long sizeLong(){
		SizeWrapper wrapper = new SizeWrapper();
		Future<?> f = threadExecutor.submit(wrapper);
		try {
			f.get();
		} catch (InterruptedException e) {
			getLog().log(Level.ERROR, "Interrupted while waiting for iterate to complete",e);
		} catch (ExecutionException e) {
			getLog().log(Level.ERROR, "Iterate failed",e);
		}
		return(wrapper.result);
	}

	
	private class ClearWrapper implements Runnable{

		public void run() {

			try{
				oos.writeObject(LUCI_Butler.ServerCommands.CLEAR);
				oos.flush();
			} catch (IOException e) {
				getLog().log(Level.ERROR, "Unable to write "+LUCI_Butler.ServerCommands.CLEAR+" command",e);
			}
			
			checkForError(ois);
				
		};
	}
	
	/**
	 * Erase all the entries in the remote database.
	 */
	public synchronized void clear(){
		ClearWrapper wrapper = new ClearWrapper();
		Future<?> f = threadExecutor.submit(wrapper);
		try {
			f.get();
		} catch (InterruptedException e) {
			getLog().log(Level.ERROR, "Interrupted while waiting for iterate to complete",e);
		} catch (ExecutionException e) {
			getLog().log(Level.ERROR, "Iterate failed",e);
		}
	}
	

	
	private void checkForError(ObjectInputStream ois){
		ServerResponse okay = null;
		try {
			okay = (ServerResponse) ois.readObject();
		} catch (IOException e) {
			getLog().log(Level.ERROR, "Unable to read a result from object input stream",e);
		} catch (ClassNotFoundException e) {
			getLog().log(Level.ERROR, "Unable to read a result from object input stream",e);
		}
		
		if(okay.equals(ServerResponse.COMMAND_SUCCESSFUL)){
			return;
		}
		
		try {
			okay = (ServerResponse) ois.readObject();
		} catch (IOException e) {
			getLog().log(Level.ERROR, "Unable to read a result from object input stream",e);
		} catch (ClassNotFoundException e) {
			getLog().log(Level.ERROR, "Unable to read a result from object input stream",e);
		}
		
		throw new RuntimeException("Bad Response from server:"+okay);
	}

}
