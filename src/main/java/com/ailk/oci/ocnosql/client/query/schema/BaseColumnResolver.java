package com.ailk.oci.ocnosql.client.query.schema;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * User: Rex wong
 * Date: 13-4-11
 * Time: 上午11:18
 * version
 * since 1.4
 */
public class BaseColumnResolver implements ColumnResolver {

    private final static Log log = LogFactory.getLog(BaseColumnResolver.class);

    private static Map<String,OCTable> cachingTableHodlers = new HashMap<String,OCTable>();

    @Override
    public OciColumn resolveColumn(HTable table, int columnIndex) {
        String tableName = Bytes.toString(table.getTableName());
        OCTable tableHolder =  cachingTableHodlers.get(tableName);
        if(tableHolder==null){
            tableHolder = resolveTableByNative(table);
        }
        return tableHolder.getColumnByIndex(columnIndex);
    }
    private OCTable resolveTableByNative(HTable table){
        try {
            OCTable tableHolder = new OCTable();

            String tableName = Bytes.toString(table.getTableName());
            HTableDescriptor hTableDescriptor = table.getTableDescriptor();
            tableHolder.setName(hTableDescriptor.getNameAsString());
            int index = 0;
            for (HColumnDescriptor hColumnDescriptor : hTableDescriptor.getFamilies()) {
                Map<ImmutableBytesWritable,ImmutableBytesWritable> columns = hColumnDescriptor.getValues();
                System.out.println("ss="+hColumnDescriptor.getNameAsString());

                for(Map.Entry<ImmutableBytesWritable,ImmutableBytesWritable> column:columns.entrySet()){
                    String value =  Bytes.toString(column.getValue().get());
                    String key = Bytes.toString(column.getKey().get());
                    System.out.println("key="+key);
                    System.out.println("value="+value);
                    OciColumn holder = new OciColumn();
                    holder.setPosition(index);
                    holder.setName(hColumnDescriptor.getNameAsString());
                    tableHolder.addColumn(holder);
                }
            }
            cachingTableHodlers.put(tableName,tableHolder);
            return tableHolder;
        }
        catch (IOException e){
            log.error(e);
        }
        return null;
    }
}
