package com.ailk.oci.ocnosql.client.importdata;

import java.io.InputStream;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class DataModel {

	public static void main(String[] args){
		
	}
	
//<tables>
//	<table name="drquery_20130822">
//		<family>
//			<name>fm1</name>
//			<column>
//				<columnName>c1</columnName>
//				<composeColumn>true</composeColumn>
//				<composeColumnName>a,b,c,d</composeColumnName>
//			</column>
//		</family>
//		<family>
//			<name>fm2</name>
//			<column>
//				<columnRef>f</columnRef>
//			</column>
//			<column>
//				<columnRef>g</columnRef>
//			</column>
//			<column>
//				<columnRef>h</columnRef>
//			</column>
//		</family>
//		<column>a,b,c,d,e,f,g,h</column>
//		<rowkey>b</rowkey>
//		<rowkeyExp>${b}${c}</rowkeyExp>
//	</table>
//</tables>
	
	public void readConf(InputStream input) {
		SAXReader reader = new SAXReader();
		Document document = null;
		try {
			document = reader.read(input);
		} catch (DocumentException e) {
			e.printStackTrace();
		}
		Element root = document.getRootElement();
		List<Element> nodeList = root.elements("table");
		for(Element table : nodeList){
			String key = table.attributeValue("name");
		}
	}
}
