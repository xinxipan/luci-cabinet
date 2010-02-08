package edu.uci.ics.luci.lucicabinet;

/**
 * An abstract class for making sure that only authorized connections are made to a LUCI_Butler service.
 * It should be subclassed and used to initialize LUCI_Butler.
 *
 */
public abstract class AccessControl {
	
	public abstract boolean allowSource(String source);
}
