package com.ailk.oci.ocnosql.client.spi;

import com.ailk.oci.ocnosql.client.ClientRuntimeException;

public class ClientConnectionException extends ClientRuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -658856508296490394L;

	public ClientConnectionException(String msg, Throwable cause) {
		super(msg, cause);
		// TODO Auto-generated constructor stub
	}

	public ClientConnectionException(String msg) {
		super(msg);
		// TODO Auto-generated constructor stub
	}

}
