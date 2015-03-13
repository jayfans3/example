/**
 * 
 */
package com.ocean.rpc.ipc;

import com.ocean.rpc.protocol.VersionProtocol;
import com.ocean.rpc.util.Conf;

/**
 * @author liujs3
 *
 */
public class Server1 {

	//start server
	/**
	 * 1.impl socketchanell
	 * 2.selector wait inputdataParam
	 * 3.add call to quence
	 * 4.call to hanlder
	 * 5.handler to response qucence
	 */
	public String start(){
		return "a";
	}
	
	public Server1(){}
	
	public Server1(Conf c,VersionProtocol cp ,Object o,String addr,String port,int num){
		/**
		 * 1.new socketchanell accpt
		 * 2.register all selector to server 
		 * 3.impl the selector invoke
		 */
		
		
		
	}
	
	public String oceantest(VersionProtocol cp ,Object o){
		return "";
	}
	
	// server execute
	public String oceantest(){
		return "";
	}
	
}
