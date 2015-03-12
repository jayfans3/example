package CloudComputingServer;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

public class IP {

public static String getIP()
{
	 return getFirst192IP();
}
	
public static String getFirst192IP()
{
	Enumeration<NetworkInterface> netInterfaces = null;  
	try {  
	    netInterfaces = NetworkInterface.getNetworkInterfaces();  
	    while (netInterfaces.hasMoreElements()) {  
	        NetworkInterface ni = netInterfaces.nextElement();  
	        Enumeration<InetAddress> ips = ni.getInetAddresses();  
	        while (ips.hasMoreElements()) {  
	        	String ip = ips.nextElement().getHostAddress();
	        	if(ip.matches("192.168.*"))
	        	{
	        		return ip;
	        	}
	        }  
	    } 
	} catch (Exception e) {  
	    e.printStackTrace();  
	}  
	return "0.0.0.0";

}

static public void main(String[] args) {
		System.out.println(getIP());
}

}

