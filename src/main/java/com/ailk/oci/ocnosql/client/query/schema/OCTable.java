package com.ailk.oci.ocnosql.client.query.schema;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Rex wong
 * Date: 13-4-11
 * Time: 上午10:36
 * version
 * since 1.4
 */
public class OCTable {
    private String name;
    private String compressType;
    private String hashType;
    private List<OciColumn> columns = new ArrayList<OciColumn>(); //按照index排序的
    private int columnSize  = 0;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    public void addColumn(String column, int columnIndex){
        OciColumn ociColumn = new OciColumn();
        ociColumn.setName(column);
        ociColumn.setPosition(columnIndex);
        columns.add(ociColumn);
        columnSize++;
    }
    public void addColumn(OciColumn ociColumn){
        columns.add(ociColumn);
        columnSize++;
    }
    public OciColumn getColumnByIndex(int index){
        return columns.get(index);
    }
    public OciColumn getColumnByName(String columnName){
        for(OciColumn ociColumn :columns){
            if(ociColumn.getName().equals(columnName)){
                return ociColumn;
            }
        }
        return null;
    }
    public int getColumnSize() {
        return columnSize;
    }

    public String getHashType() {
        return hashType;
    }

    public String getCompressType() {
        return compressType;
    }
    public int containsColumn(String columnName){
        if(StringUtils.isEmpty(columnName)){
            return -1;
        }
        int index=0;
        for(OciColumn ociColumn :columns){
            if(ociColumn.getName().equals(columnName)){
                return index;
            }
            index++;
        }
        return -1;
    }
    public List<OciColumn> getAllColumn(){
        return this.columns;
    }
}
