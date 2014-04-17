package com.ailk.oci.ocnosql.client.query;

import com.ailk.oci.ocnosql.client.ClientRuntimeException;

public class TableNotFoundException extends ClientRuntimeException {

	private static final long serialVersionUID = -9199914762223563454L;

	public TableNotFoundException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public TableNotFoundException(String msg) {
		super(msg);
	}

}
