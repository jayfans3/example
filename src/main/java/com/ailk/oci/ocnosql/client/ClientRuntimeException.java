package com.ailk.oci.ocnosql.client;

import com.ailk.oci.ocnosql.common.OCNosqlNestedRuntimeException;

public class ClientRuntimeException extends OCNosqlNestedRuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -391007123349476779L;

	public ClientRuntimeException(String msg, Throwable cause) {
		super(msg, cause);
		// TODO Auto-generated constructor stub
	}

	public ClientRuntimeException(String msg) {
		super(msg);
		// TODO Auto-generated constructor stub
	}

}
