package com.ailk.oci.ocnosql.client.put.model;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ailk.oci.ocnosql.client.config.spi.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.ailk.oci.ocnosql.client.spi.ConfigException;


/**数据结构	
<tables>
	<table name="drquery_20130822" separator="">
		<family>
			<name>fm1</name>
			<column>
				<columnName>c1</columnName>
				<composeColumn>true</composeColumn>
				<composeColumnRef>a,b,c,d</composeColumnRef>
			</column>
		</family>
		<family>
			<name>fm2</name>
			<column>
				<columnRef>f</columnRef>
			</column>
			<column>
				<columnRef>g</columnRef>
			</column>
			<column>
				<columnRef>h</columnRef>
			</column>
		</family>
		<column>a,b,c,d,e,f,g,h</column>
		<rowkeyRef>b</rowkeyRef>
		<rowkeyExp>${b}${c}</rowkeyExp>
	</table>
</tables>
*/
public class TableConfReader {
	
	
	public static Map<String, Table> readConfFromHDFSFile(String path, Configuration conf){
		return readConf(TableConfiguration.getInputStreamFromHDFS(path, conf));
	}
	
	
	public static Map<String, Table> readConfFromLocalFile(String path){
		File file = new File(path);
		InputStream is = null;
		try {
			is = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			throw new ConfigException("read file exception.", e);
		}
		return readConf(is);
	}
	
	
	@SuppressWarnings("unchecked")
	public static Map<String, Table> readConf(InputStream is){
		Map<String, Table> tableConf = new HashMap<String, Table>(); 
		try {
			SAXReader reader = new SAXReader();
			Document document = null;
			document = reader.read(is);
			Element root = document.getRootElement();
			List<Element> tableList = root.elements("table");

			if (tableList != null && tableList.size() > 0) {
				for (Element tableElement : tableList) {
					Table table = new Table();
					table.setName(tableElement.attributeValue("name"));
					table.setMd5Check(tableElement.attributeValue("md5Check"));
					table.setSeparator(tableElement.attributeValue("separator"));
                    System.out.println(tableElement.element("separator"));
					table.setColumns(tableElement.element("column").getText());
					table.setRowkeyRef(tableElement.element("rowkeyRef").getText());
					if(tableElement.element("rowkeyExp") != null){
						table.setRowkeyExp(tableElement.element("rowkeyExp").getText());
					}
					List<Element> fmList = tableElement.elements("family");
					for(Element fmElement : fmList){
						Family family = new Family();
						family.setName(fmElement.element("name").getText());
						List<Element> columnList = fmElement.elements("column");
						for(Element columElement : columnList){
							Column column = new Column();
							String val = null;
							if(columElement.element("columnName") != null){
								val = columElement.element("columnName").getText();
								if(StringUtils.isNotEmpty(val)){
									column.setColumnName(val);
								}
							}
							if(columElement.element("composeColumn") != null){
								val = columElement.element("composeColumn").getText();
								if(StringUtils.isNotEmpty(val)){
									column.setComposeColumn(Boolean.valueOf(val));
								}
							}
							if(columElement.element("composeColumnRef") != null){
								val = columElement.element("composeColumnRef").getText();
								if(StringUtils.isNotEmpty(val)){
									column.setComposeColumnRef(val);
								}
							}
							if(columElement.element("columnRef") != null){
								val = columElement.element("columnRef").getText();
								if(StringUtils.isNotEmpty(val)){
									column.setColumnRef(val);
								}
							}
							family.addColumn(column);
						}
						table.addFamily(family);
					}
					tableConf.put(table.getName(), table);
				}
			}
		} catch (DocumentException e) {
			throw new ConfigException("read tableConf exception.", e);
		} catch (Exception e1){
			throw new ConfigException(e1.getMessage(), e1);
		}
		return tableConf;
	}
	
	
	public static InputStream getInputStream(String path){
		File file = new File(path);
		InputStream is = null;
		try {
			is = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			throw new ConfigException("read file exception.", e);
		}
		return is;
	}

}