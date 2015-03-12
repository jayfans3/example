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
		    //�û���������  
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
	  
	//���ڴ�  
	MemoryUsage heapMemoryUsage = MemoryUsage  
	.from((CompositeDataSupport) mbsc.getAttribute(heapObjName,  
	        "HeapMemoryUsage"));  
	long commitMemory = heapMemoryUsage.getCommitted();// �ѵ�ǰ����  
	long usedMemory = heapMemoryUsage.getUsed();  
	System.out.print("���ڴ�����:"+heapMemoryUsage.getMax()/1024+"KB,��ǰ������:"+commitMemory/1024+"KB,��ǰʹ����:"+usedMemory/1024+"KB,");  
	System.out.println("���ڴ�ʹ����:" + (int) usedMemory * 100  
	        / commitMemory + "%");// ��ʹ����  
	  
	      //ջ�ڴ�  
	        MemoryUsage nonheapMemoryUsage = MemoryUsage  
	        .from((CompositeDataSupport) mbsc.getAttribute(heapObjName,"NonHeapMemoryUsage"));  
	        long noncommitMemory = nonheapMemoryUsage.getCommitted();  
	long nonusedMemory = heapMemoryUsage.getUsed();  
	  
	System.out.println("ջ�ڴ�ʹ����:" + (int) nonusedMemory * 100  
	        / noncommitMemory + "%");  
	          
	//PermGen�ڴ�  
	        ObjectName permObjName = new ObjectName("java.lang:type=MemoryPool,name=Perm Gen");  
	  
	MemoryUsage permGenUsage = MemoryUsage.from((CompositeDataSupport) mbsc.getAttribute(permObjName,"Usage"));  
	long committed = permGenUsage.getCommitted();// �־öѴ�С  
	long used = heapMemoryUsage.getUsed();//    
	System.out.println("perm gen:" + (int) used * 100 / committed  
	        + "%");// �־ö�ʹ����  
}
  
  public void getEden() throws InstanceNotFoundException, IntrospectionException, ReflectionException, IOException, MalformedObjectNameException, NullPointerException, AttributeNotFoundException, MBeanException{
	  ObjectName youngHeapObjName = new ObjectName("java.lang:type=MemoryPool,name=Eden Space");  
	    // ��ȡMbean����  
	    MBeanInfo youngHeapInfo = mbsc.getMBeanInfo(youngHeapObjName);  
	    // ��ȡ���������  
	    MBeanAttributeInfo[] youngHeapAttributes = youngHeapInfo.getAttributes();  
	          
	    MemoryUsage youngHeapUsage = MemoryUsage  
	                .from((CompositeDataSupport) mbsc.getAttribute(youngHeapObjName, "Usage"));  
	          
	       System.out.print("Ŀǰ�������� ������ڴ�:"+youngHeapUsage.getMax()/1024+"KB,");  
	       System.out.print("�������ѷ���:"+youngHeapUsage.getCommitted()/1024+"KB,");  
	       System.out.print("��������ʼ��:"+youngHeapUsage.getInit()/1024+"KB,");  
	       System.out.println("��������ʹ��"+youngHeapUsage.getUsed()/1024+"KB");  
	    
  }
}
