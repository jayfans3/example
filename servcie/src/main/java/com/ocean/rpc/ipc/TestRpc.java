package com.ocean.rpc.ipc;

import com.ocean.rpc.Rpc;
import com.ocean.rpc.protocol.VersionProtocol;



public class TestRpc {
	
	public static void main(String args[]){
		while(true){
		System.out.print(((VersionProtocol)Rpc.getProxy()).oceantest("a"));
	}}
}
