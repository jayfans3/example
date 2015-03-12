package namenodezk;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;

public class getIP {
	
	public static String getLocalIP(){
		String ip=null;
		try {
			ip=java.net.InetAddress.getLocalHost().getHostAddress();
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}
		
		return ip;
		
	}
	public void getallIP(){
		
		Enumeration<NetworkInterface> netInterfaces = null;  
		try {  
		    netInterfaces = NetworkInterface.getNetworkInterfaces();  
		    while (netInterfaces.hasMoreElements()) {  
		        NetworkInterface ni = netInterfaces.nextElement();  
		        System.out.println("DisplayName:" + ni.getDisplayName());  
		        System.out.println("Name:" + ni.getName());  
		        Enumeration<InetAddress> ips = ni.getInetAddresses();  
		        while (ips.hasMoreElements()) {  
		            System.out.println("IP:"  
		            + ips.nextElement().getHostAddress());  
		        }  
		    }  
		} catch (Exception e) {  
		    e.printStackTrace();  
		}  
		
	}
	

	public static void main(String[] args){
		
		/*	Enumeration<NetworkInterface> netInterfaces = null;  
	try {  
	    netInterfaces = NetworkInterface.getNetworkInterfaces();  
	    while (netInterfaces.hasMoreElements()) {  
	        NetworkInterface ni = netInterfaces.nextElement();  
	        System.out.println("DisplayName:" + ni.getDisplayName());  
	        System.out.println("Name:" + ni.getName());  
	        Enumeration<InetAddress> ips = ni.getInetAddresses();  
	        while (ips.hasMoreElements()) {  
	            System.out.println("IP:"  
	            + ips.nextElement().getHostAddress());  
	        }  
	    }  
	} catch (Exception e) {  
	    e.printStackTrace();  
	}  
	
	
	try {
		   System.out.println(java.net.InetAddress.getLocalHost().getHostAddress());
		  } catch (UnknownHostException e) {
		   // TODO Auto-generated catch block
		   e.printStackTrace();
		  } 
	}
*/
}
}