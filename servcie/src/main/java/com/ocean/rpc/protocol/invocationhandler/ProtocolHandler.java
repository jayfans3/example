/**
 * 
 */
package com.ocean.rpc.protocol.invocationhandler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import com.ocean.rpc.protocol.VersionProtocol;


/**
 * @author liujs3
 *
 */
public class ProtocolHandler implements InvocationHandler{

	private VersionProtocol v;
	public ProtocolHandler(VersionProtocol v) {
		this.v=v;
	}
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		Object o =null ;
		//TODO ocean socket
		/**
		 * 1.connection + addcall
		 * 2.sendCall
		 * 3.receiveResponse
		 * 4.delete call
		 */
		return o;
	}
}
