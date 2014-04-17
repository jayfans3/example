package com.ailk.oci.ocnosql.client.query.criterion;


import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("unchecked")
public class MinorOrEquals extends ExpressionBase {
    Comparable value;

    public MinorOrEquals(String colFamily, String col, Comparable value) {
        super(colFamily,col);
        this.value = value;
    }

    public boolean accept(Comparable detail) {

        Comparable v = null;

        if (value instanceof String) {
            return detail.compareTo(value) <= 0 ? true : false;
        }
        if (value instanceof Integer) {
            v = Integer.decode(detail.toString());
        }
        if (value instanceof Double) {
            v = Double.parseDouble(detail.toString());
        }
        if (value instanceof Long) {
            v = Long.parseLong(detail.toString());
        }
        if (value instanceof Float) {
            v = Float.parseFloat(detail.toString());
        }
        return v.compareTo(value) <= 0 ? true : false;
    }
    public Filter trans2filter(){
        return new SingleColumnValueFilter(Bytes.toBytes(colFamily),Bytes.toBytes(col), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(String.valueOf(value)));
    }
}
