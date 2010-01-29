package edu.uci.ics.luci.lucicabinet;


/** This is a class that is used by luci-cabinet to iterate over synchronized sets.
 * It is used by passing it to a database.  The database iterates overs it's members and
 * executes the IteratorWorkers methods in turn. It should be subclassed and the appropriate methods
 * overridden.
 * 
 * The initialize method is called once before iteration.  It is passed a reference to the database that
 * is being iterated over.  It may write to the database. 
 * 
 * The iterate function is called once for every element in the database.  It may not write to the database. 
 * If it tries to, then it will lock indefinitely.
 * 
 * The shutdown method is called once at the end of the iteration.  It is passed a reference to the database 
 * that was just iterated over.  It may write to the database.
 * 
 *  Although there will be no changes made to the database between calls to iterate.  There may be changes to
 * the underlying database after initialize is called and before the first call to iterate.  Likewise, there
 * may be changes to the database after the last call to iterate and the call to shutdown.
 * @author djp3
 *
 */
public class IteratorWorker {
	
	protected void initialize(HDB_LUCI parent){
	}
	
	protected void iterate(byte[] key,byte[] value){
	}
	
	protected void shutdown(HDB_LUCI parent){
	}

}
