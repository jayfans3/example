package com.ocean.rpc.ipc;


public class Call {

	private int callid;
	
	private String param;
	
	private Object o;
	
	private String error;
	
	private boolean isdone;

	public int getCallid() {
		return callid;
	}

	public void setCallid(int callid) {
		this.callid = callid;
	}

	public String getParam() {
		return param;
	}

	public void setParam(String param) {
		this.param = param;
	}

	public Object getO() {
		return o;
	}

	public void setO(Object o) {
		this.o = o;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public boolean isIsdone() {
		return isdone;
	}

	public void setIsdone(boolean isdone) {
		this.isdone = isdone;
	}
	
	
}
