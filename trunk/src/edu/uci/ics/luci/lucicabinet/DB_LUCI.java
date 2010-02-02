package edu.uci.ics.luci.lucicabinet;

/**
 * The abstract class for the databases that luci-cabinet synchronizes over
 *
 */
public abstract class DB_LUCI {

	public abstract void remove(byte[] packint);

	public abstract void put(byte[] key, byte[] value);

	public abstract byte[] get(byte[] key);
	
	public abstract void iterate(IteratorWorker iw);
	
	public abstract Long size();
	
	public abstract void close();

}
