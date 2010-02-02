package edu.uci.ics.luci.lucicabinet;

import java.io.Serializable;

/** This is a class that is used by luci-cabinet to iterate over synchronized sets.
 * One uses it by passing it to a database.  The database iterates overs it's members and
 * executes the IteratorWorker's methods in turn. It should be subclassed and the appropriate methods
 * overridden.
 * 
 * The <pre>initialize</pre> method is called once before iteration.  It is passed a reference to the database that
 * is being iterated over.  It may write to the database. 
 * 
 * The <pre>iterate</pre> function is called once for every element in the database.  It may not write to the database. 
 * If it tries to, then it will lock indefinitely.
 * 
 * The <pre>shutdown</pre> method is called once at the end of the iteration.  It is passed a reference to the database 
 * that was just iterated over.  It may write to the database.
 * 
 *  Although there will be no changes made to the database between while the <pre>initialize</pre> through the end of the
 *  <pre>iterate</pre> method, there may be changes to
 * the underlying database after the completion of <pre>iterate</pre> and the call to shutdown.
 *
 */
public class IteratorWorker implements Serializable{
	
	private static final long serialVersionUID = 8561619937201394728L;

	protected void initialize(HDB_LUCI parent){
	}
	
	protected void iterate(byte[] key,byte[] value){
	}
	
	protected void shutdown(HDB_LUCI parent){
	}

}
