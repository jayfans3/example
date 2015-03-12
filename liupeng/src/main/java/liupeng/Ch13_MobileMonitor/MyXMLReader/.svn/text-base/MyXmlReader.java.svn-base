package MyXMLReader;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class MyXmlReader {

	private Map<String, String> result;
	private String path = null;

	public void setResult(Map<String, String> result) {
		this.result = result;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}
	public MyXmlReader(String path) {
		this.path = path;
		result = new HashMap<String, String>();
		try {
			xmlReader();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
    public void  xmlReader() throws Exception{
    	
    	File input =null;
    	System.out.println("xmlPath is " + path);
	    		input = new File(path);
		    	 if(!input.exists()){
		    		 System.out.println(path+"    file no exist**********************************");
		    	 }

		    	 SAXReader reader = new SAXReader();
		    	 Document document = reader.read(input);
		    	 Element rootElm = document.getRootElement();
		    	 List<Element> children = new ArrayList<Element>();
		    	 children = rootElm.elements();
		    	 for(int i=0;i<children.size();i++){
		    	 result.put(children.get(i).getName(), children.get(i).getText().trim());
    	 }
   
    }
    
    public String getName(String name)
    {
    	return result.get(name);
    }
    
    
    //测试调用方法
	 public static void main(String[] args) throws Exception {
		 MyXmlReader reader = new MyXmlReader("d:/xml/test.xml");
	    System.out.println("ddd is " + reader.getName("ddd"));
	
	 }
}

