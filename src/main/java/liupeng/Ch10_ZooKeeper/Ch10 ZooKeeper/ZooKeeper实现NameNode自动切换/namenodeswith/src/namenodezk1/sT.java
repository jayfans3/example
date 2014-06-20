package namenodezk1;
import java.util.*;

public class sT {
	public static void main(String args[]){
		String a="123,234,345,123";
		StringTokenizer st=new StringTokenizer(a,",");
		//String b;
		StringBuilder b1=new StringBuilder();
		while (st.hasMoreElements()){
			
			String b=st.nextToken();
			System.out.println(b);
			if (b.equals("123"));
			else
				{
				//System.out.println(b);
				b1=b1.append(b+",");}
			
		}
		System.out.println(b1);
	}

}
