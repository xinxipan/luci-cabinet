package edu.uci.ics.luci.lucicabinet.library;

import edu.uci.ics.luci.lucicabinet.ShardFunction;

/** A Basic Shard Function Implementation that sends requests for a key-value pair to a shard based on the
 * hashcode of the key
 * 
 * @author djp3
 *
 */
public class ShardFunctionSimple extends ShardFunction{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 907127599147873501L;
	
	private int max;

	public ShardFunctionSimple(int numberOfShards) {
		max = numberOfShards;
	}

	@Override
	public int pickShard(Object key) {
		return (key.hashCode() % max);
	}

}
