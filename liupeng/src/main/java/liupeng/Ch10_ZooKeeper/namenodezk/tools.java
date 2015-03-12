package namenodezk;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class tools {
	
	
	public static String deleteIP(String srcIP,String desIP){
		//String a="123,234,345,123";
		StringTokenizer st=new StringTokenizer(srcIP,",");
		//String b;
		StringBuilder b1=new StringBuilder();
		while (st.hasMoreElements()){
			
			String b=st.nextToken();
			//System.out.println(b);
			if (b.equals(desIP));
			else
				{
				//System.out.println(b);
				b1=b1.append(b+",");}
			
		}
		int length=b1.length();
		b1.deleteCharAt(length-1);
		return b1.toString();
	}
	
	
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
	
	public static List<String> pareseIP(String srcIP){
		
		
		StringTokenizer st=new StringTokenizer(srcIP,",");
		//String b;
		List<String> s=new ArrayList<String>();
		StringBuilder b1=new StringBuilder();
		while (st.hasMoreElements()){
			
			s.add(st.nextToken());
			
//			String b=st.nextToken();
//			//System.out.println(b);
//			if (b.equals(desIP));
//			else
//				{
//				//System.out.println(b);
//				b1=b1.append(b+",");}
			
		}
		
		return s;
	}
	
/*	public static void main(String args[]){
		String a="192.168.1.11:9000,192.168.12.1:9000,192.168.13.1:9000";
		String b=tools.deleteIP(a,"192.168.13.1:9000");
		System.out.println(b);

	public static void main(String args[]){
		String a="192.168.1.11:9000,192.168.12.1:9000,192.168.13.1:9000";
		List<String> b=tools.pareseIP(a);
		System.out.println(b.get(0));
		System.out.println(b.get(1));
		System.out.println(b.get(2));
//		for(String c:b)
//		System.out.println(c);
		
	}*/



}
