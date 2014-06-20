import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.sun.jdmk.comm.HtmlAdaptorServer;

public class HelloDynamicAgent {
	private static String DOMAIN = "MyDynamicMBean";
	/**
	 * @param args
	 * @throws NullPointerException 
	 * @throws MalformedObjectNameException 
	 * @throws NotCompliantMBeanException 
	 * @throws MBeanRegistrationException 
	 * @throws InstanceAlreadyExistsException 
	 */
	public static void main(String[] args) throws MalformedObjectNameException, NullPointerException, InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
		//创建一个MBean服务对象，DOMAIN类似于java里面的公共package部分
		MBeanServer server = MBeanServerFactory.createMBeanServer(DOMAIN);
		//创建DynamicMBean对象
		HelloDynamic hello = new HelloDynamic();
		//创建一个web适配器服务器，表示我们MBean服务通过web形式来提供给用户管理
		HtmlAdaptorServer htmlserver = new HtmlAdaptorServer();
		htmlserver.setPort(9999);
		//ObjctName对象类似于完整的package
		ObjectName helloname = new ObjectName(DOMAIN + ":name=HelloDynamic");
		ObjectName htmlname = new ObjectName(DOMAIN + ":name=HtmlAdaptor");
		server.registerMBean(hello, helloname);
		server.registerMBean(htmlserver, htmlname);
		
		htmlserver.start();
	}
/**
 *  
运行程序，打开浏览器，输入http://localhost:9999，
即可访问管理页面，页面下方点击name=HelloDynamic，
进入MBean View，然后再操作中点击print方法，再次回到MBean 
View页面你会发现多了一个dynamicPrint方法，这个方法就是我们进行print操作时动态生成的。
 */
}