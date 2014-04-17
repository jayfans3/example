package com.ailk.oci.ocnosql.client.query.criterion;

import org.apache.hadoop.hbase.filter.Filter;

@SuppressWarnings("unchecked")
public interface Expression {

    public String getColFamily();

    public String getCol();

    public boolean accept(Comparable detail);

    public Filter trans2filter();

}