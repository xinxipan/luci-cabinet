package edu.uci.ics.luci.lucicabinet;

import java.io.Serializable;

/** This is a class that is used by luci-cabinet to iterate over synchronized sets.
 * 
 * <p>
 * One uses it by passing it to a database.  The database iterates overs it's members and
 * executes the IteratorWorker's methods in turn. It should be subclassed and the appropriate methods
 * overridden.
 * 
 * <p>
 * The initialize method is called once before iteration.  It is passed a reference to the database that
 * is being iterated over.  It may write to the database. 
 * 
 * <p>
 * The iterate function is called once for every element in the database.  It may not write to the database. 
 * If it tries to, then it will lock indefinitely.
 * 
 * <p>
 * The shutdown method is called once at the end of the iteration.  It is passed a reference to the database 
 * that was just iterated over.  It may write to the database.
 * 
 * <p>
 *  Although there will be no changes made to the database between while the initialize through the end of the
 *  iterate method, there may be changes to
 * the underlying database after the completion of iterate and the call to shutdown.
 *
 */
public class IteratorWorker implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -9210568925467264284L;

	protected void initialize(DB_LUCI parent){
	}
	
	protected void iterate(Serializable key,Serializable value){
	}
	
	protected void shutdown(DB_LUCI parent){
	}

}
