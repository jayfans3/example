package com.ailk.oci.ocnosql.client.thrift.serviceImpl;

import com.ailk.oci.ocnosql.client.config.spi.*;
import com.ailk.oci.ocnosql.client.query.*;
import com.ailk.oci.ocnosql.client.query.criterion.*;
import com.ailk.oci.ocnosql.client.spi.*;
import com.ailk.oci.ocnosql.client.thrift.exception.*;
import com.ailk.oci.ocnosql.client.thrift.service.*;
import com.google.common.collect.*;
import org.apache.thrift.*;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: lile3
 * Date: 13-11-19
 * Time: 下午2:29
 * To change this template use File | Settings | File Templates.
 */
public class HBaseServiceImpl implements HBaseService.Iface{

    ClientAdaptor adaptor;

    Connection conn = Connection.getInstance();

    public HBaseServiceImpl(){

    }

    public HBaseServiceImpl(ClientAdaptor adaptor){
       this.adaptor = adaptor;
    }

    @Override
    public List<List<String>> queryByRowkeyFir(String rowkey, List<String> tableNames, String columnValueFilter, Map<String, String> param) throws ClientRuntimeException, TException {
        try{
            System.out.println("tableName = "+tableNames.get(0) + " rowkey = " + rowkey);
            Criterion criterion = getCriterionByColumnFilter(columnValueFilter);
            List<String[]> stringArrResult = adaptor.queryByRowkey(conn,rowkey,tableNames,criterion,param);
            System.out.println("stringArrResult.size="+stringArrResult.size());
            return stringArrToList(stringArrResult);
        }catch(com.ailk.oci.ocnosql.client.ClientRuntimeException e){
            throw new ClientRuntimeException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public List<List<String>> queryByRowkeyFirCrList(String rowkey, List<String> tableNames,  List<String> columnValueFilterList, String logicalOpt, Map<String, String> param) throws ClientRuntimeException, TException {
       try{
            Criterion criterion = getCriterionByColumnFilterList(columnValueFilterList,logicalOpt);
            List<String[]> stringArrResult = adaptor.queryByRowkey(conn,rowkey,tableNames,criterion,param);
            return stringArrToList(stringArrResult);
        }catch(com.ailk.oci.ocnosql.client.ClientRuntimeException e){
            throw new ClientRuntimeException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public List<List<String>> queryByRowkeySec(String rowkey, List<String> tableNames, String columnValueFilter, Map<String, String> param, Map<String,List<String>> columnFilter) throws ClientRuntimeException, TException {
       try{
            List<com.ailk.oci.ocnosql.client.query.ColumnFamily> cfs = transferThriftCF(columnFilter);
            Criterion criterion = getCriterionByColumnFilter(columnValueFilter);
            List<String[]> stringArrResult = adaptor.queryByRowkey(conn,rowkey,tableNames,criterion,param,cfs);
            return stringArrToList(stringArrResult);
        }catch(com.ailk.oci.ocnosql.client.ClientRuntimeException e){
            throw new ClientRuntimeException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public List<List<String>> queryByRowkeySecCrList(String rowkey, List<String> tableNames, List<String> columnValueFilterList, String logicalOpt, Map<String, String> param, Map<String,List<String>> columnFilter) throws ClientRuntimeException, TException {
       try{
            List<com.ailk.oci.ocnosql.client.query.ColumnFamily> cfs = transferThriftCF(columnFilter);
            Criterion criterion = getCriterionByColumnFilterList(columnValueFilterList,logicalOpt);
            List<String[]> stringArrResult = adaptor.queryByRowkey(conn,rowkey,tableNames,criterion,param,cfs);
            return stringArrToList(stringArrResult);
        }catch(com.ailk.oci.ocnosql.client.ClientRuntimeException e){
            throw new ClientRuntimeException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public List<List<String>> queryByRowkeyThr(List<String> rowkey, List<String> tableNames, String columnValueFilter,
                                               Map<String, String> param) throws ClientRuntimeException, TException {
       try{
            Criterion criterion = getCriterionByColumnFilter(columnValueFilter);
            List<String[]> stringArrResult = adaptor.queryByRowkey(conn,transferObjectArrToStringArr(rowkey.toArray()),
                    tableNames,criterion,param);
            return stringArrToList(stringArrResult);
        }catch(com.ailk.oci.ocnosql.client.ClientRuntimeException e){
            throw new ClientRuntimeException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public List<List<String>> queryByRowkeyThrCrList(List<String> rowkey, List<String> tableNames,
                                List<String> columnValueFilterList, String logicalOpt, Map<String, String> param)
            throws ClientRuntimeException, TException {
       try{
            Criterion criterion = getCriterionByColumnFilterList(columnValueFilterList,logicalOpt);
            List<String[]> stringArrResult = adaptor.queryByRowkey(conn,transferObjectArrToStringArr(rowkey.toArray())
                    ,tableNames,criterion,param);
            return stringArrToList(stringArrResult);
        }catch(com.ailk.oci.ocnosql.client.ClientRuntimeException e){
            throw new ClientRuntimeException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public List<List<String>> queryByRowkeyPrefixFir(String rowkeyPrefix, List<String> tableNames,
                                                     String columnValueFilter, Map<String, String> param)
            throws ClientRuntimeException, TException {
        try{
            Criterion criterion = getCriterionByColumnFilter(columnValueFilter);
            List<String[]> stringArrResult = adaptor.queryByRowkeyPrefix(conn,rowkeyPrefix,tableNames,criterion,param);
            return stringArrToList(stringArrResult);
        }catch(com.ailk.oci.ocnosql.client.ClientRuntimeException e){
            throw new ClientRuntimeException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public List<List<String>> queryByRowkeyPrefixFirCrList(String rowkeyPrefix, List<String> tableNames,
                                                           List<String> columnValueFilterList, String logicalOpt, Map<String, String> param)
            throws ClientRuntimeException, TException {
       try{
            Criterion criterion = getCriterionByColumnFilterList(columnValueFilterList,logicalOpt);
            List<String[]> stringArrResult = adaptor.queryByRowkeyPrefix(conn,rowkeyPrefix,tableNames,criterion,param);
            return stringArrToList(stringArrResult);
        }catch(com.ailk.oci.ocnosql.client.ClientRuntimeException e){
            throw new ClientRuntimeException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public List<List<String>> queryByRowkeyPrefixSec(String rowkeyPrefix, List<String> tableNames,
                                           String columnValueFilter, Map<String, String> param, Map<String,List<String>> columnFilter)
            throws ClientRuntimeException, TException {
       try{
            List<com.ailk.oci.ocnosql.client.query.ColumnFamily> cfs = transferThriftCF(columnFilter);
            Criterion criterion = getCriterionByColumnFilter(columnValueFilter);
            List<String[]> stringArrResult = adaptor.queryByRowkeyPrefix(conn,rowkeyPrefix,tableNames,criterion,param,cfs);
            return stringArrToList(stringArrResult);
        }catch(com.ailk.oci.ocnosql.client.ClientRuntimeException e){
            throw new ClientRuntimeException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public List<List<String>> queryByRowkeyPrefixSecCrList(String rowkeyPrefix, List<String> tableNames,
                                  List<String> columnValueFilterList, String logicalOpt, Map<String, String> param, Map<String,List<String>> columnFilter)
            throws ClientRuntimeException, TException {
        try{
            List<com.ailk.oci.ocnosql.client.query.ColumnFamily> cfs = transferThriftCF(columnFilter);
            Criterion criterion = getCriterionByColumnFilterList(columnValueFilterList,logicalOpt);
            List<String[]> stringArrResult = adaptor.queryByRowkeyPrefix(conn,rowkeyPrefix,tableNames,criterion,param,cfs);
            return stringArrToList(stringArrResult);
        }catch(com.ailk.oci.ocnosql.client.ClientRuntimeException e){
            throw new ClientRuntimeException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public List<List<String>> queryByRowkeyFou(List<String> rowkey, List<String> tableNames, String columnValueFilter,
                                               Map<String, String> param, Map<String,List<String>> columnFilter)
            throws ClientRuntimeException, TException {
        try{
         Criterion criterion = getCriterionByColumnFilter(columnValueFilter);
        List<String[]> stringArrResult = adaptor.queryByRowkey(conn,(String[])rowkey.toArray(),tableNames,criterion,
                                                            param,transferThriftCF(columnFilter));
        return stringArrToList(stringArrResult);
        }catch(com.ailk.oci.ocnosql.client.ClientRuntimeException e){
            throw new ClientRuntimeException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public List<List<String>> queryByRowkeyFouCrList(List<String> rowkey, List<String> tableNames,
                                    List<String> columnValueFilterList, String logicalOpt, Map<String, String> param,
                                    Map<String,List<String>> columnFilter)
            throws ClientRuntimeException, TException {
        try{
        Criterion criterion = getCriterionByColumnFilterList(columnValueFilterList,logicalOpt);
        List<String[]> stringArrResult = adaptor.queryByRowkey(conn,(String[])rowkey.toArray(),tableNames,criterion,
                                                            param,transferThriftCF(columnFilter));
        return stringArrToList(stringArrResult);
        }catch(com.ailk.oci.ocnosql.client.ClientRuntimeException e){
            throw new ClientRuntimeException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public List<List<String>> queryByRowkeyFiv(String startKey, String stopKey, List<String> tableNames,
                                               String columnValueFilter, Map<String, String> param)
            throws ClientRuntimeException, TException {
        try{
            Criterion criterion = getCriterionByColumnFilter(columnValueFilter);
            List<String[]> stringArrResult = adaptor.queryByRowkey(conn,startKey,stopKey,tableNames,criterion,param);
            return stringArrToList(stringArrResult);
        }catch(com.ailk.oci.ocnosql.client.ClientRuntimeException e){
            throw new ClientRuntimeException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public List<List<String>> queryByRowkeyFivCrList(String startKey, String stopKey, List<String> tableNames,
                                                     List<String> columnValueFilterList, String logicalOpt,
                                                     Map<String, String> param)
            throws ClientRuntimeException, TException {
        try{
            Criterion criterion = getCriterionByColumnFilterList(columnValueFilterList,logicalOpt);
            List<String[]> stringArrResult = adaptor.queryByRowkey(conn,startKey,stopKey,tableNames,criterion,param);
            return stringArrToList(stringArrResult);
        }catch(com.ailk.oci.ocnosql.client.ClientRuntimeException e){
            throw new ClientRuntimeException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public List<List<String>> queryByRowkeySix(String startKey, String stopKey, List<String> tableNames,
                                            String columnValueFilter, Map<String, String> param,
                                            Map<String,List<String>> columnFilter)
            throws ClientRuntimeException, TException {
        try{
            Criterion criterion = getCriterionByColumnFilter(columnValueFilter);
            List<String[]> stringArrResult = adaptor.queryByRowkey(conn,startKey,stopKey,tableNames,criterion,param,
                    transferThriftCF(columnFilter));
            return stringArrToList(stringArrResult);
        }catch(com.ailk.oci.ocnosql.client.ClientRuntimeException e){
            throw new ClientRuntimeException().setErrormessage(e.getMessage());
        }
    }

    @Override
    public List<List<String>> queryByRowkeySixCrList(String startKey, String stopKey, List<String> tableNames,
                                   List<String> columnValueFilterList, String logicalOpt, Map<String, String> param,
                                   Map<String,List<String>> columnFilter)
            throws ClientRuntimeException, TException {
        try{
            Criterion criterion = getCriterionByColumnFilterList(columnValueFilterList,logicalOpt);
            List<String[]> stringArrResult = adaptor.queryByRowkey(conn,startKey,stopKey,tableNames,criterion,param,
                    transferThriftCF(columnFilter));
            return stringArrToList(stringArrResult);
        }catch(com.ailk.oci.ocnosql.client.ClientRuntimeException e){
            throw new ClientRuntimeException().setErrormessage(e.getMessage());
        }
    }

    public Criterion getCriterionByColumnFilter(String columnValueFilterExpress) throws ClientRuntimeException{
        if(columnValueFilterExpress == null) return null;
        String[] arr = columnValueFilterExpress.split(">=|<=|=|>|>=|<");
        if(arr==null || arr.length !=2 || arr[0].split(":")==null || arr[0].split(":").length !=2){//表达式不符合
           throw new ClientRuntimeException().setErrormessage("columnValueFilter arg is error.used as follow: family:qualifier=value");
        }
        Criterion criterion = new Criterion();
        String familyName = arr[0].split(":")[0].trim();
        String columnName = arr[0].split(":")[1].trim();
        String columnValue = arr[1].trim();
        if(columnValueFilterExpress.indexOf(">=") != -1){
           criterion.setGreaterOrEquals(familyName,columnName,columnValue);
        }else if(columnValueFilterExpress.indexOf("<=") != -1){
           criterion.setMinorOrEquals(familyName,columnName,columnValue);
        }else if(columnValueFilterExpress.indexOf("=") != -1){
           criterion.setEqualsTo(familyName, columnName, columnValue);
        }else if(columnValueFilterExpress.indexOf("<") != -1){
           criterion.setMinorThan(familyName, columnName, columnValue);
        }else if(columnValueFilterExpress.indexOf(">") != -1){
           criterion.setGreaterThan(familyName, columnName, columnValue);
        }
        return criterion;
     }

    public Criterion getCriterionByColumnFilterList(List<String> columnValueFilterList, String logicalOpt)throws ClientRuntimeException{
        Criterion criterion = new Criterion();
        if(logicalOpt.trim().equalsIgnoreCase("and")){
           criterion.setAND();
        }else if(logicalOpt.trim().equalsIgnoreCase("or")){
           criterion.setOR();
        }
        if(columnValueFilterList != null && columnValueFilterList.size() >0){
            HashMap<String, List<Expression>> opr = new HashMap<String, List<Expression>>();
            for(String columnValueFilter : columnValueFilterList){
                opr.putAll(getCriterionByColumnFilter(columnValueFilter).getOpr());
            }
            criterion.setOpr(opr);
        }
        return criterion;
    }

    private List<List<String>> stringArrToList(List<String[]> stringArrResult){
        List<List<String>> listResult = new ArrayList<List<String>>();
        if(stringArrResult == null)
            return listResult;
        for(String[] arr : stringArrResult){
            listResult.add(Arrays.asList(arr));
        }
        return listResult;
    }

    private List<ColumnFamily> transferThriftCF(Map<String,List<String>> columnFilter){
       List<ColumnFamily> queryCfs = new ArrayList<ColumnFamily>();
       if(columnFilter == null) return queryCfs;
       Set<Map.Entry<String,List<String>>> set = columnFilter.entrySet();
       for(Iterator<Map.Entry<String,List<String>>> it=set.iterator();it.hasNext();){
          Map.Entry<String,List<String>> entry = it.next();
          String family = entry.getKey();
          List<String> columns = entry.getValue();
          ColumnFamily qcf = new ColumnFamily();
          qcf.setFamily(family);
          String[] arr = new String[columns.size()];
          for(int i=0; i<arr.length; i++){
              arr[i] = columns.get(i);
          }
          qcf.setColumns(arr);
          queryCfs.add(qcf);
       }
       return queryCfs;
    }

    private String[] transferObjectArrToStringArr(Object[] arrObj){
        if(arrObj == null) return null;
        String[] arrStr = new String[arrObj.length];
        int i=0;
        for(Object obj : arrObj){
          arrStr[i++] = (String)obj;
        }
        arrObj = null;//置为null，加快回收
        return arrStr;
    }

    private Map<String,String> transferObjectMapToStringMap(Map<String,Object> objMap){
        if(objMap == null) return null;
        Map<String,String> strMap = new HashMap<String, String>();
        int i=0;
        Set<Map.Entry<String,Object>> set = objMap.entrySet();
        for(Iterator<Map.Entry<String,Object>> it = set.iterator(); it.hasNext();){
           Map.Entry<String,Object> entry = it.next();
           strMap.put(entry.getKey(),(String)entry.getValue());
        }
        objMap.clear();
        objMap = null; //置为null，加快回收
        return strMap;
    }
}
