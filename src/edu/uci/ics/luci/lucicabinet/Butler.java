package edu.uci.ics.luci.lucicabinet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Butler is a class that provides socket access to a HDB_LUCI database.
 * See the code for UseCase 3 and 4
 *
 */
public class Butler implements Runnable{
	
	enum ServerCommands {PUT,GET,REMOVE,ITERATE, CLOSE, SIZE};
	enum ServerResponse {CONNECTION_OKAY,COMMAND_SUCCESSFUL,COMMAND_FAILED};
	
	private boolean shuttingDown = false;
	protected DB_LUCI db;
	protected AccessControl checker;
	private ServerSocket serverSocket = null;
	
	private static transient volatile Logger log = null;
	public Logger getLog(){
		if(log == null){
			log = Logger.getLogger(Butler.class);
		}
		return log;
	}
	
	/**
	 * Stop this butler service.
	 */
	public void shutdown(){
		shuttingDown = true;
		if(serverSocket != null){
			synchronized(serverSocket){
				serverSocket.notifyAll();
			}
			try {
				serverSocket.close();
			} catch (IOException e) {
				getLog().log(Level.ERROR,"Could not close Server Socket");
			}
		}
	}
	
	/**
	 * 
	 * @param db The database to expose
	 * @param port The port to accept commands on
	 * @param checker An object which tells us which connections are allowed
	 */
	public Butler(DB_LUCI db,int port,AccessControl checker){
		this.db = db;
		this.checker = checker;
		try {
		    serverSocket = new ServerSocket(port);
		} catch (IOException e) {
		    getLog().log(Level.ERROR,"Could not listen on port:"+port);
		}
	}
	
	/**
	 * This method is called to begin accepting socket connections. 
	 */
	public void initialize(){
		if(serverSocket == null){
			throw new RuntimeException("Unable to start Butler object");
		}
		
		Thread t = new Thread(this);
		t.setName("Butler Socket Accept Thread");
		t.setDaemon(false); /*Force an explicit shutdown call */
		t.start();
	}

	private class Handler implements Runnable{
		
		private Socket clientSocket = null;

		public Handler(Socket clientSocket){
			this.clientSocket = clientSocket;
			
		}

		public void run() {
			boolean done = false;
			ObjectInputStream ois = null;
			ObjectOutputStream oos = null;
			
			try{
				String source = clientSocket.getInetAddress().toString();
				if(checker.allowSource(source)){
					/* Get the object input stream */
					try {
						ois = new ObjectInputStream(clientSocket.getInputStream());
					} catch (IOException e) {
						getLog().log(Level.ERROR, "Unable to create object input stream",e);
						return;
					}
				
					/* Get the object output stream */
					try {
						oos = new ObjectOutputStream(clientSocket.getOutputStream());
					} catch (IOException e) {
						getLog().log(Level.ERROR, "Unable to create object output stream",e);
						return;
					}
					
					try {
						oos.writeObject(ServerResponse.CONNECTION_OKAY);
						oos.flush();
					} catch (IOException e) {
						getLog().log(Level.ERROR, "Unable to write to object output stream",e);
					}
				
					while(!done){
						String response = "";
						
						/* Get the command */
						ServerCommands command = null;
						try {
							command = (Butler.ServerCommands) ois.readObject();
						} catch (IOException e) {
							getLog().log(Level.ERROR, "Unable to read a command from object input stream",e);
							response += e;
						} catch (ClassNotFoundException e) {
							getLog().log(Level.ERROR, "Unable to read a command from object input stream",e);
							response += e;
						}
					
						/*Process the command */
						if(command != null){
							if(command.equals(Butler.ServerCommands.REMOVE)){
								Serializable key= null;
								try {
									key  = (Serializable) ois.readObject();
								} catch (IOException e) {
									getLog().log(Level.ERROR, "Unable to read the key to remove from object input stream",e);
									response += e.toString();
								} catch (ClassNotFoundException e) {
									getLog().log(Level.ERROR, "Unable to read the key to remove from object input stream",e);
									response += e.toString();
								}
					
								if(key != null){
									/*Execute remove */
									try{
										db.remove(key);
									}
									catch(RuntimeException e){
										getLog().log(Level.ERROR, "Unable to read the remove object from database",e);
										response += e.toString();
									}
								}
							}
							else if(command.equals(Butler.ServerCommands.PUT)){
								Serializable key= null;
								try {
									key  =  (Serializable) ois.readObject();
								} catch (IOException e) {
									getLog().log(Level.ERROR, "Unable to read the key to put from object input stream",e);
									response += e.toString();
								} catch (ClassNotFoundException e) {
									getLog().log(Level.ERROR, "Unable to read the key to put from object input stream",e);
									response += e.toString();
								}
					
								Serializable value = null;
								try {
									value = (Serializable) ois.readObject();
								} catch (IOException e) {
									getLog().log(Level.ERROR, "Unable to read the value to put from object input stream",e);
									response += e.toString();
								} catch (ClassNotFoundException e) {
									getLog().log(Level.ERROR, "Unable to read the value to put from object input stream",e);
									response += e.toString();
								}
				
								/*Execute put */
								try{
									db.put(key,value);
								}
								catch(RuntimeException e){
									getLog().log(Level.ERROR, "Unable to put key-value pair into database",e);
									response += e.toString();
								}
							}
							else if(command.equals(Butler.ServerCommands.GET)){
								Serializable key= null;
								try {
									key = (Serializable) ois.readObject();
								} catch (IOException e) {
									getLog().log(Level.ERROR, "Unable to read a key to get object input stream",e);
									response += e.toString();
								} catch (ClassNotFoundException e) {
									getLog().log(Level.ERROR, "Unable to read a key to get object input stream",e);
									response += e.toString();
								} catch(RuntimeException e){
									getLog().log(Level.ERROR, "Unable to read a key to get object input stream",e);
									response += e.toString();
								}
					
								/*Execute get */
								Serializable value = db.get(key);
								try {
									oos.writeObject(value);
								} catch (IOException e) {
									getLog().log(Level.ERROR, "Unable to write a result to object output stream",e);
									response += e.toString();
								}
							}
							else if(command.equals(Butler.ServerCommands.ITERATE)){
								IteratorWorker iw= null;
								try {
									iw  = (IteratorWorker) ois.readObject();
								} catch (IOException e) {
									getLog().log(Level.ERROR, "Unable to read an Iterator Worker from object input stream",e);
									response += e.toString();
								} catch (ClassNotFoundException e) {
									getLog().log(Level.ERROR, "Unable to read an Iterator Worker from object input stream",e);
									response += e.toString();
								} catch(RuntimeException e){
									getLog().log(Level.ERROR, "Unable to read an Iterator Worker from object input stream",e);
									response += e.toString();
								}
								
								try{
									db.iterate(iw);
								}
								catch(RuntimeException e){
									getLog().log(Level.ERROR, "Unable to iterate on a database",e);
									response += e.toString();
								}

								try {
									oos.writeObject(iw);
								} catch (IOException e) {
									getLog().log(Level.ERROR, "Unable to write a result to object output stream",e);
									response += e.toString();
								}
							}
							else if(command.equals(Butler.ServerCommands.CLOSE)){
								done = true;
							}
							else if(command.equals(Butler.ServerCommands.SIZE)){
								Long ret = db.size();

								try {
									oos.writeObject(ret);
								} catch (IOException e) {
									getLog().log(Level.ERROR, "Unable to write a result to object output stream",e);
									response += e.toString();
								}
							}
							else{
								done = true;
								getLog().log(Level.ERROR, "Unknown command sent to Butler:"+command);
							}
					
							/* Return result */
							if(response.equals("")){
								try{
									oos.writeObject(ServerResponse.COMMAND_SUCCESSFUL);
								} catch (IOException e) {
									getLog().log(Level.ERROR, "Unable to write a result to object output stream",e);
									return;
								}
							}
							else{
								try{
									oos.writeObject(ServerResponse.COMMAND_FAILED);
								} catch (IOException e) {
									getLog().log(Level.ERROR, "Unable to write a result to object output stream",e);
									return;
								}
						
								try {
									oos.writeObject(response);
								} catch (IOException e) {
									getLog().log(Level.ERROR, "Unable to write a result to object output stream",e);
									return;
								}
								
								done = true;
							}
						}
					}
				}
			}
			finally{
				if(oos != null){
					try {
						oos.close();
					} catch (IOException e) {
						getLog().log(Level.ERROR, "Unable to close oos",e);
					}
				}
				if(ois != null){
					try {
						ois.close();
					} catch (IOException e) {
						getLog().log(Level.ERROR, "Unable to close ois",e);
					}
				}
				if(clientSocket != null){
					try {
						clientSocket.close();
					} catch (IOException e) {
						getLog().log(Level.ERROR, "Unable to close clientSocket",e);
					}
				}
			}
		}
	}
	

	public void run() {
		Socket clientSocket = null;
		while(!shuttingDown){
			try {
				/* Grab a connection */
				try{
					clientSocket = serverSocket.accept();
				} catch (SocketException e) {
					/* Socket closed is okay because that's what happens when Butler shuts down */
					if(e.getMessage().equals("Socket closed")){
						shuttingDown = true;
					}
					else{
						throw e;
					}
				}
				if(!shuttingDown){
					Thread t = new Thread(new Handler(clientSocket));
					t.setDaemon(false);
					t.start();
				}
			} catch (IOException e) {
				getLog().log(Level.ERROR, "Unable to accept a clientSocket",e);
			}
		}
	}
	
	public static class SimpleAccessControl extends AccessControl{
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
			return false;
		}
	}

}
