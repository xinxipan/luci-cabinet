package edu.uci.ics.luci.lucicabinet.library;

import edu.uci.ics.luci.lucicabinet.AccessControl;

/**
* This is a simple implementation of AccessControl that allows all connections
*
*/
public class SimplestAccessControl extends AccessControl {
	
	@Override
	public boolean allowSource(String source) {
		return true;
	}
}
