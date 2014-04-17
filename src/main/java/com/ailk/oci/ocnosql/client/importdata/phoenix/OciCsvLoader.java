package com.ailk.oci.ocnosql.client.importdata.phoenix;

import au.com.bytecode.opencsv.CSVReader;
import com.ailk.oci.ocnosql.client.rowkeygenerator.GetRowKeyUtil;
import com.google.common.collect.Maps;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.*;

import java.io.FileReader;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: lifei5
 * Date: 13-12-9
 * Time: 下午3:24
 * To change this template use File | Settings | File Templates.
 */
public class OciCsvLoader{
    private final PhoenixConnection conn;
	private final String tableName;
    private final List<String> columns;
    private final boolean isStrict;
    private int unfoundColumnCount;

	public OciCsvLoader(PhoenixConnection conn, String tableName, List<String> columns, boolean isStrict) {
        this.conn = conn;
		this.tableName = tableName;
		this.columns = columns;
		this.isStrict = isStrict;
	}

    /**
	 * Upserts data from CSV file. Data is batched up based on connection batch
	 * size. Column PDataType is read from metadata and is used to convert
	 * column value to correct type before upsert. Note: Column Names are
	 * expected as first line of CSV file.
	 *
	 * @param fileName
	 * @throws Exception
	 */
	public void upsert(String fileName) throws Exception {
		CSVReader reader = new CSVReader(new FileReader(fileName));
		upsert(reader);
	}

    /**
	 * Upserts data from CSV file. Data is batched up based on connection batch
	 * size. Column PDataType is read from metadata and is used to convert
	 * column value to correct type before upsert. Note: Column Names are
	 * expected as first line of CSV file.
	 *
	 * @param reader CSVReader instance
	 * @throws Exception
	 */
    public void upsert(CSVReader reader) throws Exception {
	    List<String> columns = this.columns;
	    if (columns != null && columns.isEmpty()) {
	        columns = Arrays.asList(reader.readNext());
	    }
       // System.out.println("columnInfo[index]=" + columns);
		ColumnInfo[] columnInfo = generateColumnInfo(columns);
        PreparedStatement stmt = null;
        PreparedStatement[] stmtCache = null;
		if (columns == null) {
		    stmtCache = new PreparedStatement[columnInfo.length];
		} else {
		    String upsertStatement = QueryUtil.constructUpsertStatement(columnInfo, tableName, columnInfo.length - unfoundColumnCount);
		    stmt = conn.prepareStatement(upsertStatement);
		}
		String[] nextLine;//从csv中读出来的
        String[] newLine;//加上按规则生成的ID之后的数组
		int rowCount = 0;
		int upsertBatchSize = conn.getMutateBatchSize();
		boolean wasAutoCommit = conn.getAutoCommit();
		try {
    		conn.setAutoCommit(false);
    		Object upsertValue = null;
    		long start = System.currentTimeMillis();

    		// Upsert data based on SqlType of each column
    		//==null意味着读到了文件末尾
    		while ((nextLine = reader.readNext()) != null) {
    			//进一步判断，避免空行或者空白行(按分隔符截取之后长度还为1)
    			if(nextLine.length==1&&nextLine[0].length()==0){
    				continue;
    			}
                //根据rowkey生成规则讲rowkey对应的列数据进行加工再入库
                newLine=new String[nextLine.length+1];

                //System.out.println("---------rowKey="+GetRowKeyUtil.getRowKeyByStringArr(tableName.toUpperCase(),nextLine));
                newLine[0]=GetRowKeyUtil.getRowKeyByStringArr(tableName.toUpperCase(),nextLine);

                /**
				 * src - 源数组。 srcPos - 源数组中的起始位置。 dest - 目标数组。 destPos -
				 * 目标数据中的起始位置。 length - 要复制的数组元素的数量。
				 */
				System.arraycopy(nextLine, 0, newLine, 1, nextLine.length);

                nextLine=newLine;

    		    if (columns == null) {
    		        stmt = stmtCache[nextLine.length-1];
    		        if (stmt == null) {
    	                String upsertStatement = QueryUtil.constructUpsertStatement(columnInfo, tableName, nextLine.length);
    	                stmt = conn.prepareStatement(upsertStatement);
    	                stmtCache[nextLine.length-1] = stmt;
    		        }
    		    }
    			for (int index = 0; index < columnInfo.length; index++) {
                    //System.out.println("columnInfo[index]=" + columnInfo[index]);
    			    if (columnInfo[index] == null) {
    			        continue;
    			    }
                    String line = nextLine[index];
                    Integer info = columnInfo[index].getSqlType();
                    upsertValue = convertTypeSpecificValue(line, info);
                    //System.out.println("upsertValue="+upsertValue);
    				if (upsertValue != null) {
    					stmt.setObject(index + 1, upsertValue, columnInfo[index].getSqlType());
    				} else {
    					stmt.setNull(index + 1, columnInfo[index].getSqlType());
    				}
    			}

    			stmt.execute();

    			// Commit when batch size is reached
    			if (++rowCount % upsertBatchSize == 0) {
    				conn.commit();
    				System.out.println("Rows upserted: " + rowCount);
    			}
    		}
    		conn.commit();
    		double elapsedDuration = ((System.currentTimeMillis() - start) / 1000.0);
    		System.out.println("CSV Upsert complete. " + rowCount + " rows upserted");
    		System.out.println("Time: " + elapsedDuration + " sec(s)\n");
		} finally {
		    if(stmt != null) {
		        stmt.close();
		    }
		    if (wasAutoCommit) conn.setAutoCommit(true);
		}
	}

    /**
	 * Gets CSV string input converted to correct type
	 */
	private Object convertTypeSpecificValue(String s, Integer sqlType) throws Exception {
	    return PDataType.fromSqlType(sqlType).toObject(s);
	}

	/**
	 * Get array of ColumnInfos that contain Column Name and its associated
	 * PDataType
	 *
	 * @param columns
	 * @return
	 * @throws java.sql.SQLException
	 */
	private ColumnInfo[] generateColumnInfo(List<String> columns)
			throws SQLException {
	    Map<String,Integer> columnNameToTypeMap = Maps.newLinkedHashMap();
        DatabaseMetaData dbmd = conn.getMetaData();
        // TODO: escape wildcard characters here because we don't want that behavior here
        String escapedTableName = StringUtil.escapeLike(tableName);
        String[] schemaAndTable = escapedTableName.split("\\.");
        ResultSet rs = null;
        try {
            rs = dbmd.getColumns(null, (schemaAndTable.length == 1 ? "" : schemaAndTable[0]),
                    (schemaAndTable.length == 1 ? escapedTableName : schemaAndTable[1]),
                    null);
            while (rs.next()) {
                columnNameToTypeMap.put(rs.getString(QueryUtil.COLUMN_NAME_POSITION), rs.getInt(QueryUtil.DATA_TYPE_POSITION));
            }
        } finally {
            if(rs != null) {
                rs.close();
            }
        }
        ColumnInfo[] columnType;
	    if (columns == null) {
            int i = 0;
            columnType = new ColumnInfo[columnNameToTypeMap.size()];
            for (Map.Entry<String, Integer> entry : columnNameToTypeMap.entrySet()) {
                columnType[i++] = new ColumnInfo(entry.getKey(),entry.getValue());
            }
	    } else {
            // Leave "null" as indication to skip b/c it doesn't exist
            columnType = new ColumnInfo[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                String columnName = SchemaUtil.normalizeIdentifier(columns.get(i).trim());
                Integer sqlType = columnNameToTypeMap.get(columnName);
                if (sqlType == null) {
                    if (isStrict) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_NOT_FOUND)
                            .setColumnName(columnName).setTableName(tableName).build().buildException();
                    }
                    unfoundColumnCount++;
                } else {
                    columnType[i] = new ColumnInfo(columnName, sqlType);
                }
            }
            if (unfoundColumnCount == columns.size()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_NOT_FOUND)
                    .setColumnName(Arrays.toString(columns.toArray(new String[0]))).setTableName(tableName).build().buildException();
            }
	    }
		return columnType;
	}
}
