package com.ailk.oci.ocnosql.client.put.model;

import java.util.ArrayList;
import java.util.List;
import com.ailk.oci.ocnosql.client.rowkeygenerator.GenRKStep;

public class Table {

	private String name;
	private String separator;
	private String columns;
	private String rowkeyRef;
	private String rowkeyExp;
	private String md5Check;
	private List<Family> fmList = new ArrayList<Family>();
	
	
	public void addFamily(Family family){
		fmList.add(family);
	}

	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}

	

	public String getRowkeyRef() {
		return rowkeyRef;
	}

	public void setRowkeyRef(String rowkeyRef) {
		this.rowkeyRef = rowkeyRef;
	}

	public String getRowkeyExp() {
		return rowkeyExp;
	}

	public void setRowkeyExp(String rowkeyExp) {
		this.rowkeyExp = rowkeyExp;
	}

	public List<Family> getFmList() {
		return fmList;
	}

	public void setFmList(List<Family> fmList) {
		this.fmList = fmList;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSeparator() {
		return separator;
	}

	public void setSeparator(String separator) {
		this.separator = separator;
	}

	public String getMd5Check() {
		return md5Check;
	}

	public void setMd5Check(String md5Check) {
		this.md5Check = md5Check;
	}
}
