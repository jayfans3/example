package com.ailk.oci.ocnosql.client.importdata;


import org.apache.hadoop.hbase.client.Put;

public class TablePutPair {
    private byte[] tablename;
    private Put put;

    public TablePutPair(byte[] tablename,Put put){
        this.tablename=tablename;
        this.put=put;
    }

    public byte[] getTablename() {
        return tablename;
    }

    public Put getPut() {
        return put;
    }
}
