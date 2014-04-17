package com.ailk.oci.ocnosql.client.query;

import com.ailk.oci.ocnosql.client.query.schema.OCTable;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Rex wong
 * Date: 13-4-12
 * Time: 上午11:51
 * version
 * since 1.4
 */
public class OCResultSet {
    private OCTable currentOCTable;
    private List<String[]> data = new ArrayList<String[]>();
    private long size;

    public void insertRow(String[] row){
        data.add(row);
        size++;
    }
    public void appendRows(List<String[]> rows){
        data.addAll(rows);
        size=size+rows.size();
    }
    public long getSize() {
        return size;
    }

    public List<String[]> getData() {
        return data;
    }
    public void flushData(List<String[]> toFlushData){
        data =  toFlushData;
        size=toFlushData.size();
    }
    public OCTable getCurrentOCTable() {
        return currentOCTable;
    }

    public void setCurrentOCTable(OCTable currentOCTable) {
        this.currentOCTable = currentOCTable;
    }
    public boolean isEmpty(){
        return size==0;
    }
}
