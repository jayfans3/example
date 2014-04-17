package com.ailk.oci.ocnosql.client.query.criterion;

import java.text.ParseException;
import java.util.*;

import org.apache.commons.lang.time.DateUtils;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

@SuppressWarnings("unchecked")
public class Criterion {
    /**
     * 操作类型，取值一般为and、or，分别由setAND()和setOR()方法设置
     */
    private String oprType = "and";

    private List<Criterion> queryParamList = new ArrayList<Criterion>();

    public class ExpressionPair{
        String key;
        Object expression;
        String oprType;
        String colType;
        public  ExpressionPair(String key,Object expression,String colType,String oprType) {
            this.key = key;
            this.expression = expression;
            this.colType = colType;
            this.oprType=oprType;
        }
        public String getKey() {
            return key;
        }
        public Object getExpression() {
            return expression;
        }

        public String getOprType() {
            return oprType;
        }

        public String getColType() {
            return colType;
        }
    }

    public void setQueryParam(Criterion queryParam) {
        queryParamList.add(queryParam);
    }

    public List<Criterion> getQueryParams() {
        return queryParamList;
    }

    public boolean hasNestedQueryParams() {
        return queryParamList.size() == 0 ? false : true;
    }

    HashMap<String, List<Expression>> opr = new HashMap<String, List<Expression>>();

    public void setOR() {
        this.oprType = "or";
    }

    public void setAND() {
        this.oprType = "and";
    }

    public String getOprType() {
        return oprType;
    }

    public HashMap<String, List<Expression>> getOpr() {
        return opr;
    }

    public void setOpr(HashMap<String, List<Expression>> opr) {
        this.opr = opr;
    }

    /**
     * 功能：设置列名等于列值的过滤条件，类似于sql语句中where条件中的 columnName='gprs'的过滤
     *
     * @param columnName
     * @param value
     * @return
     */
    public Criterion setEqualsTo(String columnFamily, String columnName, Comparable value) {
        if (null != this.getOpr().get(columnName)) {
            EqualsTo et = new EqualsTo(columnFamily,columnName,value);
            this.getOpr().get(columnName).add(et);
        } else {
            List list = new ArrayList();
            EqualsTo et = new EqualsTo(columnFamily,columnName,value);
            list.add(et);
            this.getOpr().put(columnName, list);
        }
        return this;
    }

    /**
     * 功能：设置列名大于列值的过滤条件，类似于sql语句中where条件中的 columnName>10的过滤
     *
     * @param columnName
     * @param value
     * @return
     */
    public Criterion setGreaterThan(String columnFamily, String columnName, Comparable value) {
        if (null != this.getOpr().get(columnName)) {
            GreaterThan et = new GreaterThan(columnFamily,columnName,value);
            this.getOpr().get(columnName).add(et);
        } else {
            List list = new ArrayList();
            GreaterThan et = new GreaterThan(columnFamily,columnName,value);
            list.add(et);
            this.getOpr().put(columnName, list);
        }
        return this;
    }

    public Criterion setGreaterOrEquals(String columnFamily, String columnName, Comparable value) {
        if (null != this.getOpr().get(columnName)) {
            GreaterOrEquals et = new GreaterOrEquals(columnFamily,columnName,value);
            this.getOpr().get(columnName).add(et);
        } else {
            List list = new ArrayList();
            GreaterOrEquals et = new GreaterOrEquals(columnFamily,columnName,value);
            list.add(et);
            this.getOpr().put(columnName, list);
        }
        return this;
    }

    /**
     * 功能：设置列名小于列值的过滤条件，类似于sql语句中where条件中的 columnName<10的过滤
     *
     * @param columnName
     * @param value
     * @return
     */
    public Criterion setMinorThan(String columnFamily, String columnName, Comparable value) {
        if (null != this.getOpr().get(columnName)) {
            MinorThan et = new MinorThan(columnFamily,columnName,value);
            this.getOpr().get(columnName).add(et);
        } else {
            List list = new ArrayList();
            MinorThan et = new MinorThan(columnFamily,columnName,value);
            list.add(et);
            this.getOpr().put(columnName, list);
        }
        return this;
    }


    public Criterion setMinorOrEquals(String columnFamily, String columnName, Comparable value) {
        if (null != this.getOpr().get(columnName)) {
            MinorOrEquals et = new MinorOrEquals(columnFamily,columnName,value);
            this.getOpr().get(columnName).add(et);
        } else {
            List list = new ArrayList();
            MinorOrEquals et = new MinorOrEquals(columnFamily,columnName,value);
            list.add(et);
            this.getOpr().put(columnName, list);
        }
        return this;
    }

    /**
     * 功能：设置列名等于列值的过滤条件，类似于sql语句中where条件中的 columnName in (1,2)的过滤
     *
     * @param columnName
     * @param value
     * @return
     */
    public Criterion setInExpression(String columnFamily, String columnName, Collection value) {
        if (null != this.getOpr().get(columnName)) {
            InExpression et = new InExpression(columnFamily,columnName,value);
            this.getOpr().get(columnName).add(et);
        } else {
            List list = new ArrayList();
            InExpression et = new InExpression(columnFamily,columnName,value);
            list.add(et);
            this.getOpr().put(columnName, list);
        }
        return this;
    }

    /**
     * 功能：根据查询条件生成filter，供OCNoSQL后台过滤。此方法仅支持多列模式，不支持单列。
     * @return
     */
    public Filter genFilter(){
        FilterList.Operator operator =  "and".equals(oprType)?FilterList.Operator.MUST_PASS_ALL:FilterList.Operator.MUST_PASS_ONE;
        List<Filter> filterList = new ArrayList<Filter>();
        Filter filter = new FilterList(operator,filterList);
        for(Map.Entry<String,List<Expression>> oprEntry:opr.entrySet()) {
            for(Expression exp : oprEntry.getValue()){
                  filterList.add(exp.trans2filter());
            }
        }
        if(queryParamList!=null){
            for(Criterion subCriterion:queryParamList) {
                filterList.add(subCriterion.genFilter());
            }
        }
        return  filter;
    }
}
