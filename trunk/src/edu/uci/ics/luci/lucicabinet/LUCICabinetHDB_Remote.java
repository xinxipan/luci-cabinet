package edu.uci.ics.luci.lucicabinet;

import java.io.IOException;
import java.io.Serializable;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

/**
 * This is a convenience class for specifying that the remote database is a tokyo cabinet Hash Database. This 
 * interface can't enforce that the remote implementation is actually an HDB, however. 
 *
 */
public class LUCICabinetHDB_Remote<K extends Serializable,V extends Serializable> extends LUCICabinetMap_Remote<K,V>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3332053791094264878L;
	
	private static transient volatile Logger log = null;
	public static Logger getLog(){
		if(log == null){
			log = Logger.getLogger(LUCICabinetHDB_Remote.class);
		}
		return log;
	}
	
	/**
	 *  This method opens a socket connection and read/write stream to a remote server running a LUCI_Butler service.
	 * @param host The remote host to connect to, e.g. "localhost", "192.128.1.20"
	 * @param port The port that the remote host is listening on.
	 * @throws UnknownHostException
	 * @throws IOException thrown if the remote host doesn't respond with the expected handshake.
	 */
	public LUCICabinetHDB_Remote(String host,Integer port) throws UnknownHostException, IOException {
		super(host,port);
	}

}