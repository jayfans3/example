package com.ailk.oci.ocnosql.client.config.spi;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.bag.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.ailk.oci.ocnosql.client.ClientRuntimeException;
import com.ailk.oci.ocnosql.client.spi.ClientConnectionException;

public class Connection {

	private final static Log log = LogFactory.getLog(Connection.class.getSimpleName());

	private static Properties confProperty;

	private Configuration conf;

	private ThreadPoolExecutor threadPool;

    private static volatile Connection INSTANCE = null;
	/**
	 * 可选的参数
	 */

	private String msg;

    public static Connection getInstance()  {
        synchronized (Connection.class) {
            if (INSTANCE == null) {
                INSTANCE = new Connection();
            }
        }
        return INSTANCE;
    }
	private Connection() {

		readConf(); //获取客户端配置文件
		connect(); //用客户端的host/port配置覆盖hbase的配置
		init(); //创建执行线程池，初始化系统参数
	}

	/**
	 * 创建执行线程池，初始化系统参数
	 */
	protected void init() {

		int corePoolSize = Integer.parseInt(get(
				CommonConstants.THREADPOOL_COREPOOLSIZE, "50"));// 默认并发运行的线程数
		int maximumPoolSize = Integer.parseInt(get(
				CommonConstants.THREADPOOL_MAXPOOLSIZE, "80"));// 默认池中最大线程数
		long keepAliveTime = Integer.parseInt(get(
				CommonConstants.THREADPOOL_KEEPALIVETIME, "60"));// 默认池中线程最大等待时间

		log.warn("[ocnosql]create default threadPool,corePoolSize="
				+ corePoolSize + " maximumPoolSize=" + maximumPoolSize
				+ " keepAliveTime=" + keepAliveTime + "s.");
		
		//创建执行线程池
		threadPool = new ThreadPoolExecutor(
				corePoolSize, // 并发运行的线程数
				maximumPoolSize, // 池中最大线程数
				keepAliveTime, // 池中线程最大等待时间
				TimeUnit.SECONDS, // 池中线程最大等待时间单位
				new LinkedBlockingQueue<Runnable>(), // 池中队列维持对象
				Executors.defaultThreadFactory(),
				new ThreadPoolExecutor.CallerRunsPolicy() // 超过最大线程数方法，此处为自动重新调用execute方法
		);

		// 重置数据压缩方式
		conf.set(CommonConstants.COMPRESSOR, get(CommonConstants.COMPRESSOR,
				CommonConstants.DEFAULT_COMPRESSOR));

		// 重置rowkey生成方式
//		conf.set(CommonConstants.ROWKEY_GENERATOR, get(
//				CommonConstants.ROWKEY_GENERATOR,
//				""));
		
		//重置导入文件分隔符
		conf.set(CommonConstants.SEPARATOR, get(
				CommonConstants.SEPARATOR,
				CommonConstants.DEFAULT_SEPARATOR)); //默认为"/t"

		//重置rpc timeout，默认为三分钟
        conf.setLong("hbase.rpc.timeout",
        		Long.parseLong(get("hbase.rpc.timeout","180000")));
        
		log.debug("[ocnosql]create default threadPool success.");
		
		//TableConfiguration.getInstance().readTableConfiguration(conf);
	}
	

	/**
	 * 连接参数设置
	 * @throws ClientConnectionException
	 */
	private void connect() throws ClientConnectionException {
		log.info("start connect ocnosql");

		String hosts;// zookeeper主机名称，多个主机名称以,号分隔开

		String port;// 客户端端口号

		hosts = get(CommonConstants.HOSTS, null); //读取zookeeper主机名称

		port = get(CommonConstants.PORT, null); //读取zookeeper端口号

		if (StringUtils.isEmpty(hosts) || StringUtils.isEmpty(port)) {
			msg = "zookeeper hosts or port must not be null";
			log.error(msg);
			throw new ClientConnectionException(msg);
		}

		conf = HBaseConfiguration.create();
		//用客户端的配置覆盖hbase的配置
		conf.set(CommonConstants.HOSTS, hosts);
		conf.set(CommonConstants.PORT, port);
		//配置hadoop url
		conf.set(CommonConstants.HDFS_URL, get(CommonConstants.HDFS_URL, null));

		log.info("connect ocnosql successful");
	}

	public static String get(String key, String defaultValue) {
		if (confProperty == null) {
			readConf();
		}
		String value = confProperty.getProperty(key);
		if (StringUtils.isEmpty(value)) {
			return defaultValue;
		}
		return value;
	}
    public Map<String,String> retrieveValueByPrefix(String prefix) {
        if (confProperty == null) {
            readConf();
        }
        Map<String,String> propertyValue = new HashMap<String,String>();
        Set<String> prpertyNames = confProperty.stringPropertyNames();
        for(String key:prpertyNames){
            if(key.startsWith(prefix)){
                propertyValue.put(key,confProperty.getProperty(key));
            }
        }
        return propertyValue;
    }
    
	/**
	 * 获取客户端配置文件
	 */
	private static void readConf() {
		log.info("[ocnosql]start read conf file " + CommonConstants.FILE_NAME + ".");
		FileInputStream fis = null;
		try {
            URL url = Connection.class.getClassLoader().getResource(CommonConstants.FILE_NAME);
            log.info("[ocnosql]start read conf file " + CommonConstants.FILE_NAME + "["+url+"].");
			InputStream in = Connection.class.getClassLoader()
					.getResourceAsStream(CommonConstants.FILE_NAME);
			if (in == null) {
				throw new ClientRuntimeException("plz check if file "
						+ CommonConstants.FILE_NAME + " in classpath!");
			}
			confProperty = new Properties();
			confProperty.load(in);
		} catch (Exception e) {
			log.error(e.getMessage());
		} finally {
			try {
				if (fis != null)
					fis.close();
			} catch (IOException e) {
				log.error(e.getMessage(), e);
			}
			log.info("[ocnosql]end read conf file " + CommonConstants.FILE_NAME + ".");
		}
	}

	public Configuration getConf() {
		return conf;
	}

	public ThreadPoolExecutor getThreadPool() {
		return threadPool;
	}

}
