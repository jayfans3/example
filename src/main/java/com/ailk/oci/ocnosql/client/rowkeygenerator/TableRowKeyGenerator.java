package com.ailk.oci.ocnosql.client.rowkeygenerator;

import org.apache.commons.lang.*;
import org.apache.hadoop.conf.*;

import java.util.*;
import com.ailk.oci.ocnosql.client.config.spi.CommonConstants;
import org.slf4j.*;
import org.apache.hadoop.hbase.util.Base64;

/**
 * Created by IntelliJ IDEA.
 * User: lile3
 * Date: 13-11-5
 * Time: 下午4:37
 * To change this template use File | Settings | File Templates.
 */
public class TableRowKeyGenerator implements RowKeyGenerator{
    Logger LOG = LoggerFactory.getLogger(TableRowKeyGenerator.class);

    //static Log LOG = LogFactory.getLog(TableRowKeyGenerator.class);

    private static Map<String, GenRKCallBack> callbackCache = new HashMap<String,GenRKCallBack>();  //<ClassName,ClassObject>

    @Override
    public Object generate(String oriRowKey) {
        return null;
    }

    @Override
    public Object generatePrefix(String oriRowKey) {
        return null;
    }

    @Override
    public String generate(String oriRowKey, String needHashValue, String[] currenRowdata, int[] posIndex, String appendValue) {
        return null;
    }



    private Configuration conf;
    private List<GenRKStep> genRKStepList;

    public TableRowKeyGenerator(org.apache.hadoop.conf.Configuration conf, java.util.List<GenRKStep> genRKStepList) {
        this.conf = conf;
        this.genRKStepList = genRKStepList;
    }

    public org.apache.hadoop.conf.Configuration getConf() {
        return conf;
    }

    public void setConf(org.apache.hadoop.conf.Configuration conf) {
        this.conf = conf;
    }

    public java.util.List<GenRKStep> getGenRKStepList() {
        return genRKStepList;
    }

    public void setGenRKStepList(java.util.List<GenRKStep> genRKStepList) {
        this.genRKStepList = genRKStepList;
    }

    private int getColumnIndexByColumnName(String columnName){
       String columnDefi = conf.get(CommonConstants.COLUMNS);
       if(!StringUtils.isEmpty(columnDefi) && !StringUtils.isEmpty(columnName)){
          String[] columnDefiArr =  columnDefi.split(",");
          for(int i=0; i<columnDefiArr.length; i++){
              //System.out.println("columnName = " + columnName + "columnDefiArr["+i+"]" + columnDefiArr[i]);
              if(columnDefiArr[i].trim().equals(columnName.trim()) || columnDefiArr[i].split(":")[1].trim().equals(columnName.trim()))
                  return i;
          }
       }
       return -1;
    }

    /**
     * 按照配置的GenRKStep构造出rowkey
     * @return rowkey
     */
    public String generateByGenRKStep(String line,boolean isput) throws RowKeyGeneratorException{
        long startTime = System.currentTimeMillis();
        StringBuffer finalRKBuf = new StringBuffer();//记录最终rowkey
        String separatorStr = conf.get(CommonConstants.SEPARATOR);
        String columnDefi = conf.get(CommonConstants.COLUMNS);
        if(separatorStr !=null && !separatorStr.equals(CommonConstants.DEFAULT_SEPARATOR)){
            if(!isput){
              separatorStr = new String(Base64.decode(separatorStr));
            }
        }
        String[] columns = line.split(separatorStr);
        if(genRKStepList == null) throw new RowKeyGeneratorException("no row key specify,please specify in shell or ocnosqlTable.xml");
        for(int i=0; i<genRKStepList.size(); i++){
            boolean isSum = false;
            StringBuffer stepRowKeyBuf = new StringBuffer();//每一step生成的RowKey
            GenRKStep step = genRKStepList.get(i);
            String rkColumns = step.getRkIndexes().toUpperCase();
            //System.out.println("rkColumns = "+rkColumns);
            String algo = step.getAlgo();
            String callBackClass = step.getCallBack();
            if(!StringUtils.isEmpty(rkColumns)){
               String[] rkColumnArr = rkColumns.split(",");
               for(String rkColumn :rkColumnArr){
                   int index = getColumnIndexByColumnName(rkColumn);
                   if(index == -1){
                       throw  new RowKeyGeneratorException("rkcolumn=" + rkColumn +" is not found in " + columnDefi);
                   }
                   stepRowKeyBuf.append(columns[index]);
               }
//               System.out.println("stepRowKeyBuf 1= "+stepRowKeyBuf.toString());
               if(!StringUtils.isEmpty(algo)){
                  RowKeyGenerator rowKeyGenerator = RowKeyGeneratorHolder.resolveGenerator(algo);
                  String algoRowKey = (String)rowKeyGenerator.generatePrefix(stepRowKeyBuf.toString());
                  stepRowKeyBuf = new StringBuffer(algoRowKey);
               }
//               System.out.println("stepRowKeyBuf 2= "+stepRowKeyBuf.toString());
               if(!StringUtils.isEmpty(callBackClass)){  // class需要做缓存
                   try{
                       GenRKCallBack callBack;
                       if(callbackCache.get(callBackClass) != null){
                          callBack = callbackCache.get(callBackClass);
                       }else {
                          Class clazz = Class.forName(callBackClass);
                          Object obj =  clazz.newInstance();
                          callBack = (GenRKCallBack)obj;
                          callbackCache.put(callBackClass,callBack);
                       }
                       String callbackRowKey = callBack.callback(stepRowKeyBuf.toString(),line);
                       stepRowKeyBuf = new StringBuffer(callbackRowKey);
                   }catch (Exception e){
                       throw new RowKeyGeneratorException("callback error,please check ocnosqlTab.xml file." + e);
                   }
               }
//               System.out.println("stepRowKeyBuf 3= "+stepRowKeyBuf.toString());
            }else{ //如果columnIndex为空，需要对前面所有步骤完成后生成的rowkey做处理
               if(i == 0){
                  throw new RowKeyGeneratorException("no specify column index as rowkey in the file of ocnosqlTab.xml");
               }
               if(!StringUtils.isEmpty(algo)){
                  RowKeyGenerator rowKeyGenerator = RowKeyGeneratorHolder.resolveGenerator(algo);
                  String algoRowKey = (String)rowKeyGenerator.generatePrefix(finalRKBuf.toString());
                  stepRowKeyBuf = new StringBuffer(algoRowKey);
                  isSum = true;
               }
               //System.out.println("stepRowKeyBuf 4= "+stepRowKeyBuf.toString());
               if(!StringUtils.isEmpty(callBackClass)){
                   try{
                       GenRKCallBack callBack;
                       if(callbackCache.get(callBackClass) != null){
                          callBack = callbackCache.get(callBackClass);
                       }else {
                          Class clazz = Class.forName(callBackClass);
                          Object obj =  clazz.newInstance();
                          callBack = (GenRKCallBack)obj;
                          callbackCache.put(callBackClass,callBack);
                       }
                       String callbackRowKey = callBack.callback(finalRKBuf.toString(),line);
                       stepRowKeyBuf = new StringBuffer(callbackRowKey);
                       isSum = true;
                   }catch (Exception e){
                       throw new RowKeyGeneratorException("callback error,please check ocnosqlTab.xml file." + e);
                   }
               }
               //System.out.println("stepRowKeyBuf 5= "+stepRowKeyBuf.toString());
            }
            if(!isSum){
              finalRKBuf.append(stepRowKeyBuf);
            }else {
              finalRKBuf = new StringBuffer(stepRowKeyBuf.toString());
            }
        }
        //System.out.println("finalRKBuf = "+finalRKBuf.toString());
        long endTime = System.currentTimeMillis();
        //System.out.println("gen one rowkey through all steps, need const " + (endTime - startTime) + "ms");
        return finalRKBuf.toString();
    }


}
