package com.ailk.oci.ocnosql.client.cache;


import com.ailk.oci.ocnosql.client.query.schema.OCTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TableMetaCache {
    private final static Log log = LogFactory.getLog(TableMetaCache.class.getSimpleName());
    private static volatile TableMetaCache INSTANCE = null;
    private Map<String, OCTable> ociTableRefCache = new ConcurrentHashMap<String, OCTable>();

    public static TableMetaCache getInstance()  {
        synchronized (TableTupleCache.class) {
            if (INSTANCE == null) {
                INSTANCE = new TableMetaCache();
            }
        }
        return INSTANCE;
    }
    public void addOneMeta2Cache(OCTable OCTablemeta){
        if(ociTableRefCache.containsKey(OCTablemeta.getName())){
            if(log.isInfoEnabled()){
                log.info("The key["+ OCTablemeta.getName()+"] ociTableRef has in Cache , " +
                        "So we will flush new tableref to cache");
            }
        }
        ociTableRefCache.put(OCTablemeta.getName(), OCTablemeta);
    }
    public boolean containCache(String cacheKey){
        return ociTableRefCache.containsKey(cacheKey);
    }
    public OCTable getTableCache(String cacheKey){
        return ociTableRefCache.get(cacheKey);
    }

}
