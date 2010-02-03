package edu.uci.ics.luci.lucicabinet;

import java.io.Serializable;

/**
 * The abstract class for the databases that luci-cabinet synchronizes over
 *
 */
public abstract class DB_LUCI {
	
	public abstract void open(String string);

	public abstract void remove(Serializable key);

	public abstract void put(Serializable key, Serializable value);

	public abstract Serializable get(Serializable key);
	
	public abstract void iterate(IteratorWorker iw);
	
	public abstract Long size();
	
	public abstract void close();


}
