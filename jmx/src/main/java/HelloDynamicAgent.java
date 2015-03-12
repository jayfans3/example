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
		//����һ��MBean�������DOMAIN������java����Ĺ���package����
		MBeanServer server = MBeanServerFactory.createMBeanServer(DOMAIN);
		//����DynamicMBean����
		HelloDynamic hello = new HelloDynamic();
		//����һ��web����������������ʾ����MBean����ͨ��web��ʽ���ṩ���û�����
		HtmlAdaptorServer htmlserver = new HtmlAdaptorServer();
		htmlserver.setPort(9999);
		//ObjctName����������������package
		ObjectName helloname = new ObjectName(DOMAIN + ":name=HelloDynamic");
		ObjectName htmlname = new ObjectName(DOMAIN + ":name=HtmlAdaptor");
		server.registerMBean(hello, helloname);
		server.registerMBean(htmlserver, htmlname);
		
		htmlserver.start();
	}
/**
 *  
���г��򣬴������������http://localhost:9999��
���ɷ��ʹ���ҳ�棬ҳ���·����name=HelloDynamic��
����MBean View��Ȼ���ٲ����е��print�������ٴλص�MBean 
Viewҳ����ᷢ�ֶ���һ��dynamicPrint��������������������ǽ���print����ʱ��̬���ɵġ�
 */
}