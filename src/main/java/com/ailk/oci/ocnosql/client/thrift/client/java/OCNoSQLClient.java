package com.ailk.oci.ocnosql.client.thrift.client.java;

import com.ailk.oci.ocnosql.client.thrift.service.*;
import org.apache.thrift.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: lile3
 * Date: 13-11-18
 * Time: 下午11:24
 * To change this template use File | Settings | File Templates.
 */
public class OCNoSQLClient {

    public static void main(String[] args) {
         TTransport transport = null;
        try {
            transport = new TSocket("localhost", 9091);
            TProtocol protocol = new TBinaryProtocol(transport);
            transport.open();

            TMultiplexedProtocol hBaseServiceImp = new TMultiplexedProtocol(protocol,"HBaseService");
            HBaseService.Client hbaseClient = new HBaseService.Client(hBaseServiceImp);
            TMultiplexedProtocol sqlServiceImp = new TMultiplexedProtocol(protocol,"SQLService");
            SQLService.Client sqlClient = new SQLService.Client(sqlServiceImp);

            /*
            //调用criterionClient的方法设置查询条件
            ColumnFilter cf = new ColumnFilter();
            cf.setColumnFamily("f");
            cf.setColumnName("age");
            cf.setValue("21");
            cf.setCmpOpt(CompareOpt.greaterOrEquals);
            //调用hbaseClient的方法设置查询条件
            String rowkey = "11fhh18910002222woman11266542023321061";
            String tableName = "people";
            List<String> tables = new ArrayList<String>();
            tables.add(tableName);
            List<List<String>> result = null; //hbaseClient.queryByRowkeyFir(rowkey,tables,cf,null);
            for(List<String> a : result){
                for(int i=0; i<a.size(); i++){
                    System.out.print(a.get(i)+" ");
                }
                 System.out.println();
            }

            String sql = "select * from student";
            List<Map<String,String>> resList = sqlClient.executeQueryRawFir(sql);
            for(Map<String,String> aList : resList){
                Set<Map.Entry<String,String>> set = aList.entrySet();
                for(Iterator<Map.Entry<String,String>> it = set.iterator();it.hasNext();){
                    Map.Entry<String,String> entry = it.next();
                    System.out.println("key =" +entry.getKey() + " value="+entry.getValue());
                }
            }
             */
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            if(transport != null){
                transport.close();
            }
        }
    }

}
