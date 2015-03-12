package CloudComputingServer;


import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class BalanceWhenFailThread implements Runnable {

	private ZooKeeper zk;
	private String path;
	private int chooseNumber;
	private String  clientTmp ="";
	public  BalanceWhenFailThread(ZooKeeper lzk,String lpath,String lclientPath)
	{
		zk = lzk;
		path = lpath;
		chooseNumber = 0;
		clientTmp = lclientPath;
	}
	
	
	public void run()
	{
		//将path下面的子文件分散到其他可用的子节点上
		List<String> subFiles = new ArrayList<String>();
		do
		{
			try {
				subFiles = zk.getChildren(path, false);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			for (int i = 0; i < subFiles.size(); i++) {
				String reslut = ChoosePath();
				String from = path + "/" + subFiles.get(i);
				String pathName = reslut + "/"+ (0 - Math.abs(Long.parseLong(subFiles.get(i))));
				System.out.println("move the file " + from +" TO " +  pathName);
				try {
					String reslutPath = zk.create(pathName, zk.getData(from, null, null), ZooDefs.Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT);
					if(reslutPath == null )
					{
						System.out.println("move the file " + from +" TO " +  pathName + " IS Failed");
					}
					zk.delete(from, -1);
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
			try {
				subFiles = zk.getChildren(path, false);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}while(subFiles.size() != 0);
	
		//将path节点删除
		try {
			zk.delete(path, -1);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	public String ChoosePath()
	{
 	   List<String> orglist = null;
	   try {
		orglist = zk.getChildren(clientTmp,false);
		while(orglist.size() == 0)
		{
        	  try {
  				Thread.sleep(1000);
  			} catch (InterruptedException e) {
  				e.printStackTrace();
  			}
			orglist = zk.getChildren(clientTmp,false);
		}
		
	} catch (KeeperException e) {
		e.printStackTrace();
	} catch (InterruptedException e) {
		e.printStackTrace();
	}
    String[] nodes = orglist.toArray(new String[orglist.size()]);
    Arrays.sort(nodes);
    
    byte[] reslut  = null;
    while(reslut  == null)
    {
    	  try {
			reslut = zk.getData( clientTmp+"/"+nodes[chooseNumber%nodes.length], false,null);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    	  chooseNumber++;
    	  if( reslut != null)
    	  {
    		  return  Bytes.toString(reslut);
    	  }
    	  try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
    
    return  Bytes.toString(reslut);
	}
	
	
}
