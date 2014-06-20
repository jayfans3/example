import java.io.IOException;
import java.lang.management.MemoryUsage;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;


public class JMXUtil {

	public static JMXConnector connector;
	public static MBeanServerConnection mbsc;
	 private static void getConnection() throws Exception {  
		    //用户名、密码  
		                 Map<String, String[]> map = new HashMap<String, String[]>();  
		    map.put("jmx.remote.credentials", new String[] { "monitorRole",  
		                    "QED" });  
		    String jmxURL = "service:jmx:rmi:///jndi/rmi://192.168.0.100:1000/jmxrmi";  
		      
		    JMXServiceURL serviceURL = new JMXServiceURL(jmxURL);  
		    connector = JMXConnectorFactory.connect(serviceURL, map);  
		    mbsc = connector.getMBeanServerConnection();  
		  
		}  
	 public static void main(String args[]){
		 
	 }
	 
	  public void getAllObjectName() throws IOException, InstanceNotFoundException, IntrospectionException, ReflectionException{
		  Set MBeanset = mbsc.queryMBeans(null, null);  
		   Iterator MBeansetIterator = MBeanset.iterator();  
		   while (MBeansetIterator.hasNext()) {  
		       ObjectInstance objectInstance = (ObjectInstance) MBeansetIterator  
		               .next();  
		       ObjectName objectName = objectInstance.getObjectName();  
		       MBeanInfo objectInfo = mbsc.getMBeanInfo(objectName);  
		       System.out.print("ObjectName:" + objectName.getCanonicalName()  
		               + ".");  
		       System.out.print("mehtodName:");  
		       for (int i = 0; i < objectInfo.getAttributes().length; i++) {  
		           System.out.print(objectInfo.getAttributes()[i].getName() + ",");  
		       }  
		       System.out.println();  
		   } 
	  }
  public void getStackMemory() throws MalformedObjectNameException, NullPointerException, AttributeNotFoundException, InstanceNotFoundException, MBeanException, ReflectionException, IOException{

	  ObjectName heapObjName = new ObjectName("java.lang:type=Memory");  
	  
	//堆内存  
	MemoryUsage heapMemoryUsage = MemoryUsage  
	.from((CompositeDataSupport) mbsc.getAttribute(heapObjName,  
	        "HeapMemoryUsage"));  
	long commitMemory = heapMemoryUsage.getCommitted();// 堆当前分配  
	long usedMemory = heapMemoryUsage.getUsed();  
	System.out.print("堆内存总量:"+heapMemoryUsage.getMax()/1024+"KB,当前分配量:"+commitMemory/1024+"KB,当前使用率:"+usedMemory/1024+"KB,");  
	System.out.println("堆内存使用率:" + (int) usedMemory * 100  
	        / commitMemory + "%");// 堆使用率  
	  
	      //栈内存  
	        MemoryUsage nonheapMemoryUsage = MemoryUsage  
	        .from((CompositeDataSupport) mbsc.getAttribute(heapObjName,"NonHeapMemoryUsage"));  
	        long noncommitMemory = nonheapMemoryUsage.getCommitted();  
	long nonusedMemory = heapMemoryUsage.getUsed();  
	  
	System.out.println("栈内存使用率:" + (int) nonusedMemory * 100  
	        / noncommitMemory + "%");  
	          
	//PermGen内存  
	        ObjectName permObjName = new ObjectName("java.lang:type=MemoryPool,name=Perm Gen");  
	  
	MemoryUsage permGenUsage = MemoryUsage.from((CompositeDataSupport) mbsc.getAttribute(permObjName,"Usage"));  
	long committed = permGenUsage.getCommitted();// 持久堆大小  
	long used = heapMemoryUsage.getUsed();//    
	System.out.println("perm gen:" + (int) used * 100 / committed  
	        + "%");// 持久堆使用率  
}
  
  public void getEden() throws InstanceNotFoundException, IntrospectionException, ReflectionException, IOException, MalformedObjectNameException, NullPointerException, AttributeNotFoundException, MBeanException{
	  ObjectName youngHeapObjName = new ObjectName("java.lang:type=MemoryPool,name=Eden Space");  
	    // 获取Mbean对象  
	    MBeanInfo youngHeapInfo = mbsc.getMBeanInfo(youngHeapObjName);  
	    // 获取对象的属性  
	    MBeanAttributeInfo[] youngHeapAttributes = youngHeapInfo.getAttributes();  
	          
	    MemoryUsage youngHeapUsage = MemoryUsage  
	                .from((CompositeDataSupport) mbsc.getAttribute(youngHeapObjName, "Usage"));  
	          
	       System.out.print("目前新生区分 配最大内存:"+youngHeapUsage.getMax()/1024+"KB,");  
	       System.out.print("新生区已分配:"+youngHeapUsage.getCommitted()/1024+"KB,");  
	       System.out.print("新生区初始化:"+youngHeapUsage.getInit()/1024+"KB,");  
	       System.out.println("新生区已使用"+youngHeapUsage.getUsed()/1024+"KB");  
	    
  }
}
