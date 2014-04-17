package com.ailk.oci.ocnosql.client;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-5-18
 * Time: 下午7:57
 * To change this template use File | Settings | File Templates.
 */
public class Constants {
    static final String INDEX_TABLE_STR ="INDEX_TestTable2";
    static final String TableName_STR = "TestTable2";
    static final byte[] INDEX_TABLE = Bytes.toBytes(INDEX_TABLE_STR);
    static final byte[] TableName = Bytes.toBytes(TableName_STR);
    static final byte[] FAMILY_NAME = Bytes.toBytes("info");
    static final byte[] QUALIFIER_NAME = Bytes.toBytes("data");
    static final byte[] INDEXED_QUALIFIER_NAME = Bytes.toBytes("index_data");
}
