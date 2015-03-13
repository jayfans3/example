/**
 * 
 */
package com.ocean.rpc.protocol.sub;

import com.ocean.rpc.protocol.VersionProtocol;

/**
 * @author liujs3
 *
 */
public class SubProtocol1 implements VersionProtocol {

	public String oceantest(String a){
		return a+" is success";
	}
}
