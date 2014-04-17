package com.ailk.oci.ocnosql.client.put.model;


public class Column {
	
	private String columnName;
	private boolean composeColumn; 
	private String composeColumnRef;
	private String columnRef;
	
	
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public boolean isComposeColumn() {
		return composeColumn;
	}
	public void setComposeColumn(boolean composeColumn) {
		this.composeColumn = composeColumn;
	}
	public String getComposeColumnRef() {
		return composeColumnRef;
	}
	public void setComposeColumnRef(String composeColumnRef) {
		this.composeColumnRef = composeColumnRef;
	}
	public String getColumnRef() {
		return columnRef;
	}
	public void setColumnRef(String columnRef) {
		this.columnRef = columnRef;
	}
	
	
}
