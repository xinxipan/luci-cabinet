package edu.uci.ics.luci.lucicabinet;

import tokyocabinet.DBM;

public class Butler {
	
	DBM local;
	
	public Butler(DBM local){
		this.local = local;
	}

}
