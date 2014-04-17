package com.ailk.oci.ocnosql.client.importdata;

import java.util.ArrayList;

public class HTableBean
{
    private String tableName;
    private String rowkeyValue;
    private String currentRowData;
    private ArrayList<byte[]> cfNames;
    private ArrayList<byte[]> colNames;

    public String getTableName()
    {
        return this.tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRowkeyValue() {
        return this.rowkeyValue;
    }

    public void setRowkeyValue(String rowkeyValue) {
        this.rowkeyValue = rowkeyValue;
    }

    public String getCurrentRowData() {
        return this.currentRowData;
    }

    public void setCurrentRowData(String currentRowData) {
        this.currentRowData = currentRowData;
    }

    public ArrayList<byte[]> getCfNames() {
        return this.cfNames;
    }

    public void setCfNames(ArrayList<byte[]> cfNames) {
        this.cfNames = cfNames;
    }

    public ArrayList<byte[]> getColNames() {
        return this.colNames;
    }

    public void setColNames(ArrayList<byte[]> colNames) {
        this.colNames = colNames;
    }
}