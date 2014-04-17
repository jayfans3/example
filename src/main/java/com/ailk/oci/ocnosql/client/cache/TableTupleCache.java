package com.ailk.oci.ocnosql.client.cache;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ailk.oci.ocnosql.client.config.spi.CommonConstants;
import com.ailk.oci.ocnosql.client.spi.ConfigException;

public class TableTupleCache {
    private final static Log log = LogFactory.getLog(TableTupleCache.class.getSimpleName());
    private static volatile TableTupleCache INSTANCE = null;
    private static String CONF_FILE = "client-runtime.properties";
    private static Properties confProperty;
    private DBHelper dbHelper;
    private Map<String, OciTableRef> cache = new HashMap<String, OciTableRef>();

    public static TableTupleCache getInstance() throws IOException {
        synchronized (TableTupleCache.class) {
            if (INSTANCE == null) {
                INSTANCE = new TableTupleCache();
                init();
            }
        }
        return INSTANCE;
    }

    public void refresh(String tableName) throws Exception {
        if (confProperty == null) {
            init();
        }
        if (confProperty == null) {
            String msg = CONF_FILE + " not add classpath. may be you need rebuild your project";
            log.error(msg);
            throw new ConfigException(msg);
        }

        String sql = "select * from OC_TABLE where TABLE_NAME='" + tableName + "' and TABLE_STATUS='Active'";
        synchronized (tableName) {
            doRefresh(sql);
        }
    }

    public void refreshAll() throws Exception {
        if (confProperty == null) {
            init();
        }
        if (confProperty == null) {
            String msg = CONF_FILE + " not add classpath. may be you need rebuild your project";
            log.error(msg);
            throw new ConfigException(msg);
        }

        String sql = "select * from OC_TABLE where TABLE_STATUS='Active'";
        synchronized (cache) {
            doRefresh(sql);
        }
    }

    public OciTableRef get(String key) {
        OciTableRef tableInternal = null;
        synchronized (key) {
            tableInternal = cache.get(key);
        }
        return tableInternal;
    }

    private static void init() throws IOException {
        URL url = TableTupleCache.class.getClassLoader().getResource(CONF_FILE);
        File file = new File(url.getPath());
        FileInputStream fis = new FileInputStream(file);

        confProperty = new Properties();
        confProperty.load(fis);
    }

    private void doRefresh(String sql) throws Exception {
        String driver = confProperty.getProperty(CommonConstants.DB_DRIVERCLASSNAME);
        if (StringUtils.isEmpty(driver)) {
            String msg = "can not find " + CommonConstants.DB_DRIVERCLASSNAME + " in " + CONF_FILE;
            log.error(msg);
            throw new ConfigException(msg);
        }
        String url = confProperty.getProperty(CommonConstants.DB_URL);
        if (StringUtils.isEmpty(url)) {
            String msg = "can not find " + CommonConstants.DB_DRIVERCLASSNAME + " in " + CONF_FILE;
            log.error(msg);
            throw new ConfigException(msg);
        }
        String dbname = confProperty.getProperty(CommonConstants.DB_USERNAME);
        if (StringUtils.isEmpty(dbname)) {
            String msg = "can not find " + CommonConstants.DB_DRIVERCLASSNAME + " in " + CONF_FILE;
            log.error(msg);
            throw new ConfigException(msg);
        }
        String dbpass = confProperty.getProperty(CommonConstants.DB_PASSWORD);
        if (StringUtils.isEmpty(dbpass)) {
            String msg = "can not find " + CommonConstants.DB_DRIVERCLASSNAME + " in " + CONF_FILE;
            log.error(msg);
            throw new ConfigException(msg);
        }

        dbHelper = new DBHelper(driver, url, dbname, dbpass);
        List<OciTableRef> tableList = dbHelper.executeQuery(sql);

        if (tableList == null || tableList.size() == 0) {
            log.warn("no table to refresh");
        }

        for (OciTableRef table : tableList) {
            cache.put(table.getName(), table);
        }
    }

    public Properties getConfProperty() {
        return confProperty;
    }

}
