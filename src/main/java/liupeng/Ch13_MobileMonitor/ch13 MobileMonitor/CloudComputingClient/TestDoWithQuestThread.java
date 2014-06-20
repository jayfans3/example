package CloudComputingClient;

import java.util.Random;
import java.util.StringTokenizer;

public class TestDoWithQuestThread  implements Runnable {
	
	String  fileName = "";
	ClientTest ct = null;
	String content ="";
	public TestDoWithQuestThread( ClientTest lct , String lfileName , String lcontent)
	{
		fileName = lfileName;
		ct = lct;
		content = lcontent;
	}
	
	public void run()
	{
		
		 Random rnd = new Random();
		try {
			Thread.sleep(40000+rnd.nextInt(20)*1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		StringTokenizer as = new StringTokenizer(content,"@");
		while(as.hasMoreTokens())
		{
			String fileName = as.nextToken().replace("#", "/");
			int number   =  Integer.parseInt(as.nextToken());
			System.out.println(" fileName  is " + fileName +"  number  is " +number );
		}
		
		ct.RequestIsOver(fileName);
	}
}
