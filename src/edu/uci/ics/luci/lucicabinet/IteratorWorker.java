package edu.uci.ics.luci.lucicabinet;

import java.io.Serializable;

/** This is a class that is used by luci-cabinet to iterate over synchronized sets.
 * 
 * <p>
 * One uses it by passing the IteratorWorker class to a LUCICabinetMap.  The LUCICabinetMap, instantiates the class,
 * iterates overs it's members and executes the IteratorWorker's methods in turn:
 * initialize
 * 	iterate
 * 	combine
 * shutdown
 * 
 * <p>
 * It should be subclassed and the appropriate methods overridden.
 * 
 * <p>
 * Although there will be no changes made to the database between initialization and through the end of the
 * iterate method, there may be changes to the underlying database after the completion of iterate and the call to shutdown.
 * 
 * <p>
 * Because a particular call to LUCICabinetMap.iterate be used in concurrent contexts, it is important that any state that is
 * maintained internally in IteratorWorker is maintained in concurrent-safe data-structures (so that simultaneous calls to iterate
 * don't cause problems). Likewise because IteratorWorker might be sent across the network to work on portions of the database at a 
 * time, the combine function needs to take any state in the IteratorWorker that is passed and incorporate
 * it into its own state.
 * 
 */
public abstract class IteratorWorker<K extends Serializable,V extends Serializable> implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2938222273210746601L;
	
	/**
	 * The iterator worker can't be initialized with any parameters.
	 * Any configuration must be passed as an IteratorWorkerConfig to
	 * LUCICabinetMap.iterate
	 */
	protected IteratorWorker(){};

	/**
	 * 
	 * The initialize method is called once before iteration.  It is passed a reference to the database that
	 * is being iterated over.  It may write to the parent database. 
	 * 
	 * @param parent
	 * @param iwc A class containing configuration parameters.
	 */
	protected void initialize(LUCICabinetMap<K,V> parent,IteratorWorkerConfig iwc){ };
	
	/**
	 * The iterate function is called once for every element in the database.  It may not write to the database. 
	 * If it tries to, then it will lock indefinitely.
	 * <p>
	 * If this function returns true then iteration will attempt to stop at some undefined point in the future. It
	 * is a request to stop for efficiency, not a guarantee of stoppage.
	 * 
	 * @param key The key currently being iterated over.
	 * @param value The associated value for the key
	 * @return true if the iteration should stop, false if the iteration should continue
	 */
	protected abstract boolean iterate(K key,V value);
	
	/**	
	 * The combine method is called to join separate iterations over portions of the whole database. It needs to take any state 
	 * from the <param>iw</param> parameter passed and incorporate it into itself.
	 * 
	 * @param iw
	 */
	protected abstract void combine(IteratorWorker<K,V> iw);
	
	/**
	 * The shutdown method is called once at the end of the iteration.  It is passed a reference to the database 
	 * that was just iterated over.  It may write to the parent database.
	 * 
	 * @param parent
	 */
	protected void shutdown(LUCICabinetMap<K,V> parent){ };
}
