package com.ailk.oci.ocnosql.client.compress;

import com.ailk.oci.ocnosql.client.ClientRuntimeException;


public class CompressException extends ClientRuntimeException {
	private static final long serialVersionUID = -1123879359302749543L;

	public CompressException(String msg) {
		super(msg);
	}

	public CompressException(String msg, Throwable cause) {
		super(msg, cause);
	}

	@Override
	public String getMessage() {
		return super.getMessage();
	}

}
