package edu.uci.ics.luci.lucicabinet;

import java.io.Serializable;

public abstract class ShardFunction implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4651516754251263L;

	/**
	 * Given an object this function must return an integer that tells which shard a key is located in.
	 * It is supposed to be overriden for more efficient implementations.
	 * @param key The object to shard.
	 * @return an integer between 0 and the maximum number of shards.
	 */
	public abstract int pickShard(Object key);

}
