package edu.uci.ics.luci.lucicabinet;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

/**
 * This is a convenience class for specifying that the remote database is a tokyo cabinet Hash Database. This 
 * interface can't enforce that the remote implementation is actually an HDB, however. 
 *
 */
public class HDB_LUCI_Remote extends DB_LUCI_Remote{
	
	private static transient volatile Logger log = null;
	public Logger getLog(){
		if(log == null){
			log = Logger.getLogger(HDB_LUCI_Remote.class);
		}
		return log;
	}
	
	/**
	 *  This method opens a socket connection and read/write stream to a remote server running a Butler service.
	 * @param host The remote host to connect to, e.g. "localhost", "192.128.1.20"
	 * @param port The port that the remote host is listening on.
	 * @throws UnknownHostException
	 * @throws IOException thrown if the remote host doesn't respond with the expected handshake.
	 */
	public HDB_LUCI_Remote(String host,Integer port) throws UnknownHostException, IOException {
		super(host,port);
		open(host);
	}

}