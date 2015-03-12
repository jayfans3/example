package namenodezk1;

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
	private String Standby_To_PrimaryCommand;

	public primarynamenode(String zkhost){
		
		this.M=new addMemmber();
		this.path="/namenode";
		this.zkhost=zkhost;
		this.flag=false;
		
		this.myIP=getIP.getLocalIP();
		this.port="9000";
		this.startPrimaryCommand="/usr/local/hadoop-0.20.1-dev/bin/hadoop avatar -zero";
		this.startStandbyCommand="/usr/local/hadoop-0.20.1-dev/bin/bin/hadoop avatar -one -standby -sync";
		this.Standby_To_PrimaryCommand="/usr/local/hadoop-0.20.1-dev/bin/hadoop avatar -zero";//wo bu zhi dao zhe tiao ming ling ,xu yao xiu gai
		
		
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
				System.out.println("--------the namenode are runing ,now start as standby,waiting for being active namenode-----");
			    String nowIP=M.getetdate(path);
			   //check ip,start standby
			   if (!nowIP.equals(myIP))
			   {
				   new Thread(new exeShell(startStandbyCommand));
				   Thread.sleep(5000);
				   P_or_S=2;
				   System.out.println(myIP+":start as standby and waiting for being active namenode-----");
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
					new Thread(new exeShell(startPrimaryCommand));
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
						new Thread(new exeShell(Standby_To_PrimaryCommand));
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


