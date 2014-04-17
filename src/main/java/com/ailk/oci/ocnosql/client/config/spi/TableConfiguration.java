package com.ailk.oci.ocnosql.client.config.spi;

import com.ailk.oci.ocnosql.client.put.model.*;
import com.ailk.oci.ocnosql.client.rowkeygenerator.*;
import com.ailk.oci.ocnosql.client.spi.*;
import com.ailk.oci.ocnosql.common.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.dom4j.*;
import org.dom4j.io.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import  org.apache.commons.lang.*;


/**
 * 功能：表配置信息，配置文件为ocnosqlTable.xml
 * 
 * @author zhuangyang
 * 
 */
public class TableConfiguration {
	private static Log log = LogFactory.getLog(TableConfiguration.class);

	private static TableConfiguration tableConf = new TableConfiguration();
	private Map<String, Map<String, String>> tableCache = new HashMap<String, Map<String, String>>();
	private Map<String, Table> tableConfCache = new HashMap<String, Table>();
    private Map<String, List<GenRKStep>> genRKStepMap = new HashMap<String,List<GenRKStep>>(); //<tablename,genRKStepList>
    private static volatile long xmlLastModifyTime = 0L;
	private String CONF_FILE = "ocnosqlTable.xml";

	private TableConfiguration() {
	}

	public static TableConfiguration getInstance() {
		return tableConf;
	}

	/**
	 * 功能：读取表的配置信息，存入缓存中
	 * 
	 * @throws Exception
	 * @throws IOException
	 * @throws DocumentException 
	 */
	@SuppressWarnings("unchecked")
	public void readTableConfiguration(Configuration conf) throws ConfigException {
		FSDataInputStream fis = null;

		Path path = new Path(conf.get(CommonConstants.HDFS_URL) + "/ocnosqlConf/" + CONF_FILE);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e1) {
			throw new ConfigException("could not connect file system : " + conf.get("fs.default.name"), e1);
		}
		if (fs == null) {
			throw new ConfigException("file system object is null,please check " + CommonConstants.FILE_NAME);
		}
		try {
			if (!fs.exists(path)) {
				throw new ConfigException("could not found file :" + fs.getUri().toString() + "/ocnosqlConf/" + CONF_FILE);
			}
		} catch (IOException e2) {
			throw new ConfigException("check file :" + fs.getUri().toString() + "/ocnosqlConf/" + CONF_FILE + " occur error", e2);
		}
		try {
			fis = fs.open(path, 1024);
		} catch (IOException e1) {
			throw new ConfigException("open file:" + fs.getUri().toString() + "/ocnosqlConf/" + CONF_FILE + " occur error.", e1);
		}
		try {
			SAXReader reader = new SAXReader();
			Document document = null;
			document = reader.read(fis);
			Element root = document.getRootElement();
			List<Element> nodeList = root.elements("table");
			if (nodeList != null && nodeList.size() > 0) {
				for (Element element : nodeList) {
					if (element.attributeCount() == 0) continue;
					Map<String, String> map = new ConcurrentHashMap<String, String>();
					String key = element.attribute(0).getValue();
					for (int i = 1; i < element.attributeCount(); i++) {
						if (element.attribute(i).getName().equalsIgnoreCase(CommonConstants.SEPARATOR) && element.attribute(i).getValue().equals("\\t")) {
                            map.put(element.attribute(i).getName(), "\t");
						} else {
                            //System.out.println("===---" + element.attribute(i).getName() + "====---"+element.attribute(i).getValue());
							map.put(element.attribute(i).getName(), element.attribute(i).getValue());
						}
					}
					tableCache.put(key, map);
                    //解析genrkstep,配置好genrkstepList
                    parseRKStep(key,element);

				}


			}
		} catch (DocumentException e) {
			throw new ConfigException("update table cache failed.please check " + fs.getUri().toString() + "/ocnosqlConf/" + CONF_FILE, e);
		} catch (Exception e1){
			throw new ConfigException(e1.getMessage(), e1);
		}
	}

    public void writeTableConfiguration(String tableName, String columns, String seperator, Configuration conf){
		Map<String, String> params = new HashMap<String, String>();
		params.put(CommonConstants.TABLE_NAME, tableName);
		params.put(CommonConstants.COLUMNS, columns);
		params.put(CommonConstants.SEPARATOR, seperator);
		writeTableConfiguration(params, conf);
	}

	/**
	 * 功能：向配置文件中写入表的配置信息，同时更新缓存
	 * 
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public void writeTableConfiguration(Map<String, String> table, Configuration conf) {
		String tableName = table.get(CommonConstants.TABLE_NAME);
		String columns = table.get(CommonConstants.COLUMNS);
		String seperator = table.get(CommonConstants.SEPARATOR);
		FSDataOutputStream output = null;
		FSDataInputStream fis = null;
		XMLWriter writer = null;
		boolean motify = true;
        boolean isTableExists = false;
		try {
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path("/ocnosqlConf/" + CONF_FILE);

			SAXReader reader = new SAXReader();
			writer = new XMLWriter();
			Document doc = null;
			Element root = null;
			if (!fs.exists(path)) {
				output = fs.create(path);
				doc = DocumentHelper.createDocument();
				doc.setXMLEncoding("utf-8");
				root = doc.addElement("tables");
                writeTableConfInfoToOcnosqlTableXml(root,table,conf);
                isTableExists = true;
			} else {
				fis = fs.open(path, 1024);
				doc = reader.read(fis);
				root = doc.getRootElement();
				List<Element> es = root.elements();
				for (Element ele : es) {
					if (ele.attribute(CommonConstants.TABLE_NAME).getValue().equals(tableName)
							&& ele.attribute(CommonConstants.COLUMNS).getValue().equals(columns)
							&& ele.attribute(CommonConstants.SEPARATOR).getValue().equals(seperator)
                            && !isGenRKStepChanged(conf,ele)) {// 查找表配置文件中是否记录了该表结构信息相同，则不再记录
						motify = false;
						break;
					} else if (ele.attribute(CommonConstants.TABLE_NAME).getValue().equals(tableName)
							    && ( !ele.attribute(CommonConstants.COLUMNS).getValue().equals(columns)
                                      || !ele.attribute(CommonConstants.SEPARATOR).getValue().equals(seperator)
                                      || isGenRKStepChanged(conf,ele)
                                   )
                                ){// 查找表配置文件中是否记录了该表结构信息不同，则记录
						ele.attribute(CommonConstants.COLUMNS)
								.setValue(columns);
						ele.attribute(CommonConstants.SEPARATOR).setValue(
								seperator);
                        //System.out.println(" ======2 ");
                        root.remove(ele);//删除表配置信息
                        writeTableConfInfoToOcnosqlTableXml(root,table,conf);//将新的表配置信息添加到配置文件ocnosqlTable.xml中。
						output = fs.create(path, true);
                        isTableExists = true;
                        break;
					}
				}
			}
            if(!isTableExists){
              writeTableConfInfoToOcnosqlTableXml(root,table,conf);
              output = fs.create(path, true);
            }
			writer.setOutputStream(output);
			writer.write(doc);
			output.flush();
            xmlLastModifyTime = getHDFSFileLastModifyTime(conf,path);
		} catch (Exception e) {
			log.error("[ocnosql]write conf file /ocnosqlConf/" + CONF_FILE
					+ " occur error:", e);
		} finally {
			try {
				if (writer != null) {
					writer.close();
				}
				if (output != null) {
					output.close();
				}
				if (fis != null) {
					fis.close();
				}
				// 更新缓存
				if (motify)
					readTableConfiguration(conf);
			} catch (Exception e) {
				log.error("close stream occur error:", e);
			}
		}
	}

    private void writeTableConfInfoToOcnosqlTableXml(Element root,Map<String, String> table,Configuration conf){
      	Element tableElement = root.addElement("table").addAttribute(
						CommonConstants.TABLE_NAME,
						table.get(CommonConstants.TABLE_NAME)).addAttribute(
						CommonConstants.COLUMNS,
						table.get(CommonConstants.COLUMNS)).addAttribute(
						CommonConstants.SEPARATOR,
						table.get(CommonConstants.SEPARATOR));
        addGenRKStep(conf,tableElement);
    }

    private boolean isGenRKStepChanged(Configuration conf,Element tableEle){
        String rowKeyColumn = conf.get(CommonConstants.ROWKEYCOLUMN);
        String rowKeyGenerator = conf.get(CommonConstants.ROWKEYGENERATOR);
        String algoColumn = conf.get(CommonConstants.ALGOCOLUMN);
        String callback = conf.get(CommonConstants.ROWKEYCALLBACK);
        List<Element> genRKStepList = tableEle.elements();
        if(genRKStepList == null || genRKStepList.size()!=3 ) return true;
        boolean step1=true,step2=true,step3=true;
        for(int i=0; i<genRKStepList.size(); i++){
           if(i==0){
               Element stepEle = genRKStepList.get(i);
               Element rowKeyGeneratorEle =  stepEle.element(CommonConstants.ALGORITHM);
               Element algoColumnnEle = stepEle.element(CommonConstants.COLUMNINDEX);
               if(rowKeyGeneratorEle==null && StringUtils.isEmpty(rowKeyGenerator)){
                   if(algoColumnnEle==null && StringUtils.isEmpty(algoColumn)){
                      step1 = false;
                      continue;
                   }
               }
               if((algoColumnnEle != null && !StringUtils.isEmpty(algoColumn) && algoColumnnEle.getTextTrim().equals(algoColumn.trim()))
                       && (rowKeyGeneratorEle != null &&  !StringUtils.isEmpty(rowKeyGenerator)
                            && rowKeyGeneratorEle.getTextTrim().equals(rowKeyGenerator.trim()))){
                    step1 = false;
                    continue;
               }
           }
           if(i==1){
               Element stepEle = genRKStepList.get(i);
               Element rowkeyColumnEle =  stepEle.element(CommonConstants.COLUMNINDEX);
               if((rowkeyColumnEle==null && StringUtils.isEmpty(rowKeyColumn))
                       || (rowkeyColumnEle!=null && !StringUtils.isEmpty(rowKeyColumn) &&
                            rowkeyColumnEle.getTextTrim().equals(rowKeyColumn.trim()))){
                      step2 = false;
                      continue;
               }
           }
           if(i==2){
              Element stepEle = genRKStepList.get(i);
              Element callbackEle =  stepEle.element(CommonConstants.CALLBACK);
              if((callbackEle==null && StringUtils.isEmpty(callback))|| (callbackEle!=null && !StringUtils.isEmpty(callback)
                     && callbackEle.getTextTrim().equals(callback.trim())))
                   step3= false;
           }
        }
        //System.out.println("isGenRKStepChanged = " + (step1 || step2 || step3));
        return step1 || step2 || step3;
    }

    private void addGenRKStep(Configuration conf,Element tableElement){
        String rowKeyColumn = conf.get(CommonConstants.ROWKEYCOLUMN);
        String rowKeyGenerator = conf.get(CommonConstants.ROWKEYGENERATOR);
        String algoColumn = conf.get(CommonConstants.ALGOCOLUMN);
        String callback = conf.get(CommonConstants.ROWKEYCALLBACK);
        if(!StringUtils.isEmpty(algoColumn)){
            Element genrkstepEle = tableElement.addElement(CommonConstants.GENROWKEYSTEP);
            Element columnIndexEle = genrkstepEle.addElement(CommonConstants.COLUMNINDEX);
            columnIndexEle.setText(algoColumn);
            if(!StringUtils.isEmpty(rowKeyGenerator)){
                Element algoEle = genrkstepEle.addElement(CommonConstants.ALGORITHM);
                algoEle.setText(rowKeyGenerator);
            }
        }
        if(!StringUtils.isEmpty(rowKeyColumn)){
           Element genrkstepEle2 = tableElement.addElement(CommonConstants.GENROWKEYSTEP);
           Element columnIndexEle2 = genrkstepEle2.addElement(CommonConstants.COLUMNINDEX);
           columnIndexEle2.setText(rowKeyColumn);
        }
        if(!StringUtils.isEmpty(callback)){
           Element genrkstepEle3 = tableElement.addElement(CommonConstants.GENROWKEYSTEP);
           Element callbackEle = genrkstepEle3.addElement(CommonConstants.CALLBACK);
           callbackEle.setText(callback);
        }
    }

	public String getTableInfo(String tableName, String key, Configuration conf) {
		if(tableCache.isEmpty()){ // 如果table cache为空，则重新更新缓存
			TableConfiguration.getInstance().readTableConfiguration(conf);
		}
		if (!tableCache.containsKey(tableName)) {
			readTableConfiguration(conf);
		}
		Map<String, String> tableMap = tableCache.get(tableName);
		if (tableMap == null || tableMap.size() == 0) {
			throw new ConfigException("[ocnosql]does not find table "
					+ tableName + " in the file /ocnosqlConf/" + CONF_FILE);
		}
		return tableMap.get(key);
	}
	
    public List<GenRKStep> getTableGenRKSteps(String tableName,Configuration conf) {
		if(genRKStepMap.isEmpty() || !genRKStepMap.containsKey(tableName)){
            log.info("start read table config file.");
			readTableConfiguration(conf);
		}
        //取的时候要判断是否更改过，取文件最后修改时间
        try {
            Path path = new Path(conf.get(CommonConstants.HDFS_URL) + "/ocnosqlConf/" + CONF_FILE);
            if(getHDFSFileLastModifyTime(conf,path) != xmlLastModifyTime){
                System.out.println("config file have changed.");
                readTableConfiguration(conf);
            }
        } catch (IOException e) {
            throw new ConfigException("could not connect file system : " + conf.get("fs.default.name"), e);
        }
        return genRKStepMap.get(tableName);
	}

    private long getHDFSFileLastModifyTime(Configuration conf,Path path) throws IOException{
          FileSystem fs = FileSystem.get(conf);
          FileStatus fileStatus = fs.getFileStatus(path);
          return fileStatus.getModificationTime();
    }

	public Table getTableConf(String tableName, String path, Configuration conf){
		Table tableConf = tableConfCache.get(tableName);
		if(tableConf  == null){
			synchronized(tableConfCache){
				tableConf = tableConfCache.get(tableName);
				if(tableConf == null){
					tableConfCache = TableConfReader.readConfFromHDFSFile(path, conf);
					tableConf = tableConfCache.get(tableName);
				}
			}
		}
		return tableConf;
	}


	
	
	/**
	 * 保证只有一个线程在写文件
	 * @param tableName
	 * @param path
	 * @param tableConfStr
	 * @return
	 */
	public Table insertTableConf(String tableName, String path, String tableConfStr, Configuration conf){
		Table tableConf = tableConfCache.get(tableName);
		if(tableConf != null)
			return tableConf;
		synchronized(tableConfCache){
			tableConf = tableConfCache.get(tableName);
			if(tableConf == null){
				//write conf
				writeTableNode(tableConfStr, path, false, conf);
				//refresh conf
				tableConfCache = TableConfReader.readConfFromHDFSFile(path, conf);
				tableConf = tableConfCache.get(tableName);
			}
		}
		return tableConf;
	}
	
	
	/**
	 * 保证只有一个线程在更新文件，
	 * @param tableName
	 * @param path
	 * @param tableConfStr
	 * @return
	 */
	public Table updateTableConf(String tableName, String path, String tableConfStr, Configuration conf){
		Table tableConf = null;
		synchronized(tableConfCache){
			tableConf = tableConfCache.get(tableName);
			String md5 = MD5Util.md5(tableConfStr);
			if(md5.equals(tableConf.getMd5Check())){
				return tableConf;
			}
			//update conf
			writeTableNode(tableConfStr, path, true, conf);
			//refresh cache
			tableConfCache = TableConfReader.readConfFromHDFSFile(path, conf);
			tableConf = tableConfCache.get(tableName);
		}
		return tableConf;
	}
	
	
	/**
	 * 插入或更新conf
	 * @param tableConfStr
	 * @param path
	 * @param update	插入还是更新
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public boolean writeTableNode(String tableConfStr, String path, boolean update, Configuration conf){
		InputStream is = new ByteArrayInputStream(tableConfStr.getBytes());
		SAXReader reader = new SAXReader();
		Document document = null;
		try {
			document = reader.read(is);
		} catch (DocumentException e) {
			throw new ConfigException("convert tableConfStr to xml exception.", e);
		}
		Element tableNode = document.getRootElement();
		tableNode.addAttribute("md5Check", MD5Util.md5(tableConfStr));
		
		InputStream isOld = null;
		isOld = getInputStreamFromHDFS(path, conf);
		
		SAXReader readerOld = new SAXReader();
		Document documentOld = null;
		try {
			documentOld = readerOld.read(isOld);
		} catch (DocumentException e) {
			throw new ConfigException("convert tableConfStr to xml exception.", e);
		}
		Element rootOld = documentOld.getRootElement();
		if(update){
			List<Element> tableList = rootOld.elements("table");
	
			if (tableList != null && tableList.size() > 0) {
				for (Element tableElement : tableList) {
					String tableName = tableElement.attributeValue("name");
					if(tableName.equals(tableNode.attributeValue("name"))){
						rootOld.remove(tableElement);
						rootOld.add(tableNode);
						break;
					}
				}
			}
		}else{
			rootOld.add(tableNode);
		}
		XMLWriter writer = new XMLWriter();
		FSDataOutputStream output = null;
		FileSystem fs;
		Path p = new Path(path);
		
		try{
			fs = FileSystem.get(conf);
			output = fs.create(p, true);
			writer.setOutputStream(output);
			output = fs.create(p, true);
			writer.write(documentOld);
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			try {
				writer.close();
			} catch (IOException e) {
			}
		}
		return true;
	}
	
	
	
	/**
	 * 
	 * @param filePath
	 * @param conf
	 * @return
	 */
	public static FSDataInputStream getInputStreamFromHDFS(String filePath, Configuration conf){
		Path path = new Path(filePath);
		FileSystem fs = null;
		FSDataInputStream fis = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e1) {
			throw new ConfigException("could not connect file system : " + conf.get("fs.default.name"), e1);
		}
		if (fs == null) {
			throw new ConfigException("file system object is null,please check " + CommonConstants.FILE_NAME);
		}
		
		try {
			if (!fs.exists(path)) {
				throw new ConfigException("could not found file :" + path.toUri().getPath());
			}
		} catch (IOException e2) {
			throw new ConfigException("check file :" +  path.toUri().getPath() + " occur error", e2);
		}
		try {
			fis = fs.open(path, 1024);
		} catch (IOException e1) {
			throw new ConfigException("open file:" +  path.toUri().getPath() + " occur error.", e1);
		}
		return fis;
	}
	

    private void parseRKStep(String tableName,Element table) {
       List<Element> genrksteps = table.elements(CommonConstants.GENROWKEYSTEP);
       if (genrksteps != null && genrksteps.size() > 0) {
                List<GenRKStep> genRKStepList = new ArrayList<GenRKStep>();
				for (Element element : genrksteps) {
                    Element columnIndexEle = element.element(CommonConstants.COLUMNINDEX);
                    String columnIndex = columnIndexEle==null?"":columnIndexEle.getStringValue();
                    Element alforithmEle = element.element(CommonConstants.ALGORITHM);
                    String algo = alforithmEle==null?"":alforithmEle.getStringValue();
                    Element callbackEle = element.element(CommonConstants.CALLBACK);
                    String callback = callbackEle==null?"":callbackEle.getStringValue();
                    GenRKStep step = new GenRKStep(columnIndex,algo,callback);
                    genRKStepList.add(step);
                    //log.info("tableName is "+tableName+"columnIndex is " + columnIndex + " algo is " +algo + " callback is "+callback);
				}
                genRKStepMap.put(tableName,genRKStepList);
		}
    }

    public Map<String, Map<String, String>> getTableCache(String tableName, Configuration conf) {
        if(!tableCache.isEmpty() && tableCache.containsKey(tableName)){
            return tableCache;
		}
        //取的时候要判断是否更改过，取文件最后修改时间
        try {
            Path path = new Path(conf.get(CommonConstants.HDFS_URL) + "/ocnosqlConf/" + CONF_FILE);
            if(getHDFSFileLastModifyTime(conf,path) != xmlLastModifyTime){
                System.out.println("config file have changed.");
                readTableConfiguration(conf);
            }
        } catch (IOException e) {
            throw new ConfigException("could not connect file system : " + conf.get("fs.default.name"), e);
        }
        return tableCache;
    }

    public void setTableCache(Map<String, Map<String, String>> tableCache) {
        this.tableCache = tableCache;
    }

    public static void main(String[] args){
		TableConfiguration conf = TableConfiguration.getInstance();
		String tableConfStr = "<table name=\"drquery_20130822\"><column>a,b,c,d,e,f,g,h--</column></table>\r\n\t";
		String path = "e:/put_model.xml";
		conf.writeTableNode(tableConfStr, path, true, null);
	}
}
