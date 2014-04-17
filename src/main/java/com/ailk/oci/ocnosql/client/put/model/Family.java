package com.ailk.oci.ocnosql.client.put.model;

import java.util.ArrayList;
import java.util.List;

public class Family {

	private String name;
	
	private List<Column> colList = new ArrayList<Column>();
	
	
	
	public void addColumn(Column column){
		colList.add(column);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Column> getColList() {
		return colList;
	}

	public void setColList(List<Column> colList) {
		this.colList = colList;
	}
	
}
