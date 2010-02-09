package edu.uci.ics.luci.lucicabinet.library;

import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.uci.ics.luci.lucicabinet.AccessControl;
import edu.uci.ics.luci.lucicabinet.LUCI_Butler;

/**
* This is a simple implementation of AccessControl that takes a list of allowed connections
* in the constructor.  The object can then be passed to LUCI_Butler for access control.
* See Use Case 3 for an example of usage.
*
*/
public class SimpleAccessControl extends AccessControl {
	
	private static transient volatile Logger log = null;
	public static Logger getLog(){
		if(log == null){
			log = Logger.getLogger(LUCI_Butler.class);
		}
		return log;
	}

	private Set<String> whitelist;

	public SimpleAccessControl(Set<String> whitelist){
		this.whitelist = whitelist;
	}

	@Override
	public boolean allowSource(String source) {
		for(String white:whitelist){
			if(white.equals(source)){
				return true;
			}
		}
		getLog().log(Level.INFO, this.getClass().getCanonicalName()+" rejected a connection from "+source);
		return false;
	}
}
