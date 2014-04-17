package com.ailk.oci.ocnosql.client.rowkeygenerator;

import com.ailk.oci.ocnosql.client.config.spi.*;
import com.ailk.oci.ocnosql.client.put.model.*;
import org.apache.commons.lang.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: lile3
 * Date: 13-11-20
 * Time: 上午1:00
 * To change this template use File | Settings | File Templates.
 */
public class GetRowKeyUtil {

    static HashMap<String,List<GenRKStep>> genRowKeyGeneratorCache = new HashMap<String, List<GenRKStep>>();
    static Map<String, Map<String,String>> tableConfCache = new HashMap<String,Map<String,String>>();
    static Map<String,Configuration> confMap = new HashMap<String, Configuration>();
     /**
     * rowkey生成规则
     * @param tableName 表名
     * @param arr (key=columnQulifier value=columnVal)
     * @return
     * @throws RowKeyGeneratorException
     */
    public static String getRowKeyByStringArr(String tableName,String[] arr) throws RowKeyGeneratorException{
       if(tableName == null || arr == null || arr.length == 0) return null;
       Configuration conf = confMap.get(tableName);
       List<GenRKStep> genRKStepList = genRowKeyGeneratorCache.get(tableName);
       Map<String,String> table = tableConfCache.get(tableName);
       if(conf==null || genRKStepList == null || table == null){
           conf = Connection.getInstance().getConf();
           table = TableConfiguration.getInstance().getTableCache(tableName,conf).get(tableName);
           genRKStepList = TableConfiguration.getInstance().getTableGenRKSteps(tableName,conf);
           if(genRKStepList==null || genRKStepList.size()==0){
               throw new RowKeyGeneratorException("no configuration info of "+tableName +" in configuration in ocnosqlTable.xml");
           }
           tableConfCache.put(tableName,table);
           genRowKeyGeneratorCache.put(tableName,genRKStepList);
           //System.out.println(table.get(CommonConstants.COLUMNS) +"====" +table.get(CommonConstants.SEPARATOR));
           conf.set(CommonConstants.COLUMNS,table.get(CommonConstants.COLUMNS));
           conf.set(CommonConstants.SEPARATOR,table.get(CommonConstants.SEPARATOR));
           confMap.put(tableName,conf);
       }
       TableRowKeyGenerator generator = new TableRowKeyGenerator(conf,genRKStepList);
       return generator.generateByGenRKStep(StringUtils.join(arr,table.get(CommonConstants.SEPARATOR)),true);
    }



    /**
     * rowkey生成规则
     * @param tableName 表名
     * @param columnMap (key=columnQulifier value=columnVal)
     * @return
     * @throws RowKeyGeneratorException
     */
    public static String getRowKeyByTableName(String tableName,Map<String,String> columnMap) throws RowKeyGeneratorException{
       if(columnMap ==null) return "";
       List<GenRKStep> genRKStepList = genRowKeyGeneratorCache.get(tableName);
       if(genRKStepList == null){
           Configuration conf = Connection.getInstance().getConf();
           genRKStepList = TableConfiguration.getInstance().getTableGenRKSteps(tableName,conf);
           if(genRKStepList==null || genRKStepList.size()==0){
               throw new RowKeyGeneratorException("no configuration info of "+tableName +" in configuration in ocnosqlTable.xml");
           }
           genRowKeyGeneratorCache.put(tableName,genRKStepList);
       }
       StringBuffer rowkeyBuf = new StringBuffer();
       int i=0;
       for(GenRKStep genRKStep : genRKStepList){
           String algoColumnQualifier = genRKStep.getRkIndexes();
           if(!StringUtils.isEmpty(algoColumnQualifier)){
              String[] algoColumnArr =  algoColumnQualifier.split(",");
              StringBuffer algoBuf = new StringBuffer();
              for(String col : algoColumnArr){
                 String columnRK = (columnMap.get(col)==null||columnMap.get(col).trim().equalsIgnoreCase("null")?"":columnMap.get(col));
                 algoBuf.append(columnRK);
                 if(i>0){
                     rowkeyBuf.append(columnRK);
                 }
              }
              String algo = genRKStep.getAlgo();
              if(!StringUtils.isEmpty(algo)){
                  RowKeyGenerator rowKeyGenerator = RowKeyGeneratorHolder.resolveGenerator(algo);
                  String algoRowKey = (String)rowKeyGenerator.generatePrefix(algoBuf.toString());
                  rowkeyBuf.append(algoRowKey);
              }
           }
           i++;
       }
       return rowkeyBuf.toString();
    }
}
