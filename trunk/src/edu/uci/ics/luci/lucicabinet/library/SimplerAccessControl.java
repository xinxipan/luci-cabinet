package edu.uci.ics.luci.lucicabinet.library;

import edu.uci.ics.luci.lucicabinet.AccessControl;

/**
* This is a simple implementation of AccessControl that allows connections from localhost only
*
*/
public class SimplerAccessControl extends AccessControl {
	@Override
	public boolean allowSource(String source) {
		if(source.equals("/127.0.0.1")){
			return true;
		}
		else if(source.equals("127.0.0.1")){
			return true;
		}
		else{
			return false;
		}
	}
};
