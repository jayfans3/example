package com.ailk.oci.ocnosql.client.rowkeygenerator;

public class RowKeyGeneratorException extends RuntimeException{
	private static final long serialVersionUID = 4692595700093708408L;

    static {
        // Eagerly load the NestedExceptionUtils class to avoid classloader deadlock
        // issues on OSGi when calling getMessage(). Reported by Don Brown; SPR-5607.
        RowKeyGeneratorException.class.getName();
    }
	public RowKeyGeneratorException(String msg) {
		super(msg);
	}
	public RowKeyGeneratorException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
