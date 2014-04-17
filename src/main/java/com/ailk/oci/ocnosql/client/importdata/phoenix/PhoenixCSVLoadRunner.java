package com.ailk.oci.ocnosql.client.importdata.phoenix;
import java.io.File;
import java.io.FileReader;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import com.ailk.oci.ocnosql.client.jdbc.pool.DbPool;
import com.google.common.collect.Lists;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.util.PhoenixRuntime;
import com.salesforce.phoenix.util.SchemaUtil;
class PhoenixCSVLoadRunner implements Callable<String>{
	public final static String JDBC_PROTOCOL = "jdbc:phoenix";
    public final static char JDBC_PROTOCOL_TERMINATOR = ';';
    public final static char JDBC_PROTOCOL_SEPARATOR = ':';
	private static final String UPGRADE_OPTION = "-u";
    private static final String TABLE_OPTION = "-t";
    private static final String HEADER_OPTION = "-h";
    private static final String STRICT_OPTION = "-s";
    private static final String HEADER_IN_LINE = "in-line";
    private static final String SQL_FILE_EXT = ".sql";
    private static final String CSV_FILE_EXT = ".csv";
    private String zk;
	private String sqlPath;
	private String csvPath;
	private CountDownLatch runningThreadNum;

	public PhoenixCSVLoadRunner(String zk,String sqlPath,String csvPath){
		this.zk=zk;
		this.sqlPath=sqlPath;
		this.csvPath=csvPath;
	}
	
	public PhoenixCSVLoadRunner(String zk,String sqlPath,String csvPath,CountDownLatch runningThreadNum){
		this.zk=zk;
		this.sqlPath=sqlPath;
		this.csvPath=csvPath;
		this.runningThreadNum=runningThreadNum;
	}
	
	protected void load(String zk,String sqlPath,String csvPath) throws Exception {
		//验证参数是否正确
		if(null==zk||zk.length()<=0){
			throw new RuntimeException("zk Can't be null or empty");
		}
		if(null==sqlPath||sqlPath.length()<=0){
			throw new RuntimeException("sqlPath Can't be null or empty");
		}
		if(!sqlPath.endsWith(SQL_FILE_EXT)){
			throw new RuntimeException("sqlPath must be endsWith "+SQL_FILE_EXT);
		}
		if(null==csvPath||csvPath.length()<=0){
			throw new RuntimeException("csvPath Can't be null or empty");
		}
		if(!csvPath.endsWith(CSV_FILE_EXT)){
			throw new RuntimeException("csvPath must be endsWith "+CSV_FILE_EXT);
		}
		//验证sqlPath文件是否存在
		File sqlFile=new File(sqlPath);
		if(!sqlFile.exists()){
			throw new RuntimeException("File Not Found:["+sqlPath+"]");
		}
		//验证csvPath文件是否存在
		File csvFile=new File(csvPath);
		if(!csvFile.exists()){
			throw new RuntimeException("File Not Found:["+csvPath+"]");
		}
		//验证csvPath的文件名是否在sqlfile中有对应一致的表名
		/*
		FileReader fr=null;
		BufferedReader bin=null;
		try {
			fr = new FileReader(sqlFile);
			bin=new BufferedReader(fr);
			
			String line=null;
			while ((line = bin.readLine()) != null) {
				//System.out.println(line);
				//验证代码
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(fr!=null){
				try {
					fr.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(bin!=null){
				try {
					bin.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		*/
		List<String> list = new ArrayList<String>();
		list.add(zk);
		list.add(sqlPath);
		list.add(csvPath);
		String[] arrs = list.toArray(new String[list.size()]);
		
		//调用自己的mian方法(把PhoenixRuntime.mian()做了一些修改，原来导入完成之后直接退出了虚拟机)
		main(arrs);
	}
	
	private static void usageError() {
        System.err.println("Usage: psql [-t table-name] [-h comma-separated-column-names | in-line] <zookeeper>  <path-to-sql-or-csv-file>...\n" +
                "  By default, the name of the CSV file is used to determine the Phoenix table into which the CSV data is loaded\n" +
                "  and the ordinal value of the columns determines the mapping.\n" +
                "  -t overrides the table into which the CSV data is loaded\n" +
                "  -h overrides the column names to which the CSV data maps\n" +
                "     A special value of in-line indicating that the first line of the CSV file\n" +
                "     determines the column to which the data maps.\n" +
                "  -s uses strict mode by throwing an exception if a column name doesn't match during CSV loading.\n" +
                "Examples:\n" +
                "  psql localhost my_ddl.sql\n" +
                "  psql localhost my_ddl.sql my_table.csv\n" +
                "  psql -t my_table my_cluster:1825 my_table2012-Q3.csv\n" +
                "  psql -t my_table -h col1,col2,col3 my_cluster:1825 my_table2012-Q3.csv\n"
        );
        System.exit(-1);
    }
	
	/**
     * Provides a mechanism to run SQL scripts against, where the arguments are:
     * 1) connection URL string
     * 2) one or more paths to either SQL scripts or CSV files
     * If a CurrentSCN property is set on the connection URL, then it is incremented
     * between processing, with each file being processed by a new connection at the
     * increment timestamp value.
     */
    public static void main(String [] args) throws Exception{
        if (args.length < 2) {
            usageError();
        }
        PhoenixConnection conn = null;
        try {
            String tableName = null;
            List<String> columns = null;
            boolean isStrict = false;
            boolean isUpgrade = false;

            int i = 0;
            for (; i < args.length; i++) {
                if (TABLE_OPTION.equals(args[i])) {//-t指定要导入的表名
                    if (++i == args.length || tableName != null) {
                        usageError();
                    }
                    tableName = args[i];
                } else if (HEADER_OPTION.equals(args[i])) {//-h指定列明(逗号分隔)--会覆盖csv文件第一行的列名
                    if (++i >= args.length || columns != null) {
                        usageError();
                    }
                    String header = args[i];
                    if (HEADER_IN_LINE.equals(header)) {//in-line
                        columns = Collections.emptyList();
                    } else {
                        columns = Lists.newArrayList();
                        StringTokenizer tokenizer = new StringTokenizer(header,",");
                        while(tokenizer.hasMoreTokens()) {
                            columns.add(tokenizer.nextToken());
                        }
                    }
                } else if (STRICT_OPTION.equals(args[i])) {//-s严格模式
                    isStrict = true;
                } else if (UPGRADE_OPTION.equals(args[i])) {//-u升级
                    isUpgrade = true;
                } else {
                    break;
                }
            }
            if (i == args.length) {
                usageError();
            }
            
            Properties props = new Properties();
            if (isUpgrade) {
                props.setProperty(SchemaUtil.UPGRADE_TO_2_0, Integer.toString(SchemaUtil.SYSTEM_TABLE_NULLABLE_VAR_LENGTH_COLUMNS));
            }
            String connectionUrl = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + args[i++];
            //conn = DriverManager.getConnection(connectionUrl, props).unwrap(PhoenixConnection.class);
            //改为从线程池获取连接
            conn = DbPool.getConnection().unwrap(PhoenixConnection.class);
            
            if (SchemaUtil.upgradeColumnCount(connectionUrl, props) > 0) {
                SchemaUtil.upgradeTo2(conn);
                return;
            }
            
            for (; i < args.length; i++) {
                String fileName = args[i];
                if (fileName.endsWith(SQL_FILE_EXT)) {//.sql
               		PhoenixRuntime.executeStatements(conn, new FileReader(args[i]), Collections.emptyList());
                } else if (fileName.endsWith(CSV_FILE_EXT)) {//.csv
                    if (tableName == null) {
                        tableName = fileName.substring(fileName.lastIndexOf(File.separatorChar) + 1, fileName.length()-CSV_FILE_EXT.length());
                    }
                    OciCsvLoader csvLoader = new OciCsvLoader(conn, tableName, columns, isStrict);
                    csvLoader.upsert(fileName);
                } else {
                    usageError();
                }
                Long scn = conn.getSCN();
                // If specifying SCN, increment it between processing files to allow
                // for later files to see earlier files tables.
                
                if (scn != null) {
                    scn++;
                    props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, scn.toString());
                    //conn.close();
                    DbPool.closeConnection();
                    conn = DriverManager.getConnection(connectionUrl, props).unwrap(PhoenixConnection.class);
                }
            }
        }finally {
        	DbPool.closeConnection();
        	/*
            if(conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    //going to shut jvm down anyway. So might as well feast on it.
                }
            }
            */
        	//导入完成不要直接退出虚拟机
            //System.exit(0);
        }
    }

	@Override
	public String call() throws Exception {
		//导入数据
		try{
			//log.info("...start load["+csvPath+"]...");
			load(zk, sqlPath, csvPath);
		} catch (Exception e) {
			e.printStackTrace();
			return "...load failed["+csvPath+"]...";
		}finally{
			runningThreadNum.countDown();
		}
	
		return "...load success["+csvPath+"]...";
	}
    
}