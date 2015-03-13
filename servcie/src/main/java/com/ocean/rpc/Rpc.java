package com.ocean.rpc;

import java.lang.reflect.Proxy;

import com.ocean.rpc.protocol.VersionProtocol;
import com.ocean.rpc.protocol.invocationhandler.ProtocolHandler;
import com.ocean.rpc.protocol.sub.SubProtocol1;

/**
 * @author liujs3
 */
public class Rpc {
	public static Object getProxy(){
		SubProtocol1 cp1 = new SubProtocol1();
		ProtocolHandler handler = new ProtocolHandler(cp1);
		VersionProtocol proxy = (VersionProtocol)Proxy.newProxyInstance(cp1.getClass().getClassLoader(), cp1.getClass().getInterfaces(), handler);
		return proxy;
	}
}
