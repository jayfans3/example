package namenodezk1;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

public class Driver {

	public static void main(String[] args) throws InterruptedException{
		
		
		primarynamenode pn=new primarynamenode(args[0]);
		pn.startORsetPrimary();
		System.out.println("keeper link to zkhost:"+args[0]);
		Thread.sleep(Long.MAX_VALUE);
			
}
}
 