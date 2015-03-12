package namenodezk;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

public class primarynamenode {

	
	private addMemmber M;
	private String path;
	private boolean flag;
	private String zkhost;
	private String myIP;
	private String port;
	private int P_or_S=1;
	
	private String startPrimaryCommand;
	private String startStandbyCommand;
	private String Primary_To_StandbyCommand;

	public primarynamenode(String zkhost){
		
		this.M=new addMemmber();
		this.path="/namenode";
		this.zkhost=zkhost;
		this.flag=false;
		
		this.myIP=getIP.getLocalIP();
		
		if(this.myIP.equals("192.168.1.11"))
		{		this.num="-zero";}
		else if(this.myIP.equals("192.168.1.12"))
		{this.num="-one";}
		
		this.port="9000";
		this.startPrimaryCommand="hadoop org.apache.hadoop.hdfs.server.namenode.AvatarNode -zero";
		this.startStandbyCommand="hadoop org.apache.hadoop.hdfs.server.namenode.AvatarNode -one -standby -sync";
		this.Primary_To_StandbyCommand="hadoop org.apache.hadoop.hdfs.AvatarShell -setAvatar primary";
		
	}
	//public void run() {
		public void startORsetPrimary() {
		
		try {
			try {
				System.out.println("try to link to zkhost:"+zkhost);
				M.connect(zkhost);
				
			} catch (IOException e1) {
				
				
				System.out.println("cannot link to zkhost:"+zkhost);
				e1.printStackTrace();
			}
			//the znode is exiting,check the ip.
			flag=M.isNodeExit(path);
			if(flag){
				System.out.println("--------the primary are runing ,now start as standby,waiting for being primary-----");
			    String nowIP=M.getetdate(path);
			   //check ip,start standby
			   if (!nowIP.equals(myIP))
			   {
				  if (M.isNodeExit("/standby"))
				  {
					  
					  String exitIp=M.getetdate("/standby");//read the date from exit znode
					  String allIp=exitIp+","+myIP+":"+port;//append myip and port
					  
					  M.setdate("/standby", allIp);
				  }
				  else
				   M.joinOne("/standby");//create znode
				  
				   M.setdate("/standby", myIP+":"+port);
				  
				   new Thread(new exeShell(startStandbyCommand)).start();
				  
				   Thread.sleep(5000);
				   P_or_S=2;
				   System.out.println(myIP+":start as standby and waiting for being active primary-----");
			   }
			}
			
			
			while(flag){
				flag=M.isNodeExit(path);
				Thread.sleep(2000);//check the namenode every two second
			}
			if(!flag){
				if(P_or_S==1){
				System.out.println("--------the primary namenode is not start,ready to start-----");
			     try {
					
					M.joinOne(path);
					
					M.setdate(path, myIP+":"+port);
					new Thread(new exeShell(startPrimaryCommand)).start();
					Thread.sleep(5000);
					System.out.println();
					System.out.println(myIP+"start as primary-----");
					System.out.println();
					System.out.println("now the znodedate is: "+M.getetdate(path));
					
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					M.close();
				}
			
			}
				
				if(P_or_S==2){
					System.out.println("--------the primary namenode is down,ready to set to primary-----");
				     try {
						
						M.joinOne(path);
						
						M.setdate(path, myIP+":"+port);
						
						new Thread(new exeShell(Primary_To_StandbyCommand)).start();
						
						M.setdate("/standby", tools.deleteIP(M.getetdate("/standby"), myIP));
						
						Thread.sleep(5000);
						System.out.println();
						System.out.println(myIP+"set to  primary-----");
						System.out.println();
						System.out.println("now the znodedate is: "+M.getetdate(path));
						
						
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						M.close();
					}
					
				}
			}
			
			
			
		} catch (KeeperException e) {
			
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
			
		

	}

}