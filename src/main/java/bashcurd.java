import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.jasper.tagplugins.jstl.core.Url;



public class bashcurd {

	/**
	 * sh hadoop dfs -lsr /
	 * sh hadoop dfs -mkdir 
	 * sh hadoop dfs -rmr
	 * sh hadoop dfs -put /home/grid/.. oceanup
	 * sh hadoop dfs -get /home/grid/.. oceandown
	 * sh hadoop dfs -cat /home/grid/.. 
	 * sh hadoop dfsadmin -report
	 * sh hadoop dfsadmin -savemode leave / -savemode enter
	 * start-all.sh add node start-balancer.sh
	 * @throws IOException 
	 * 
	 * 
	 * 
	 * Usage: hadoop [--config confdir] COMMAND
where COMMAND is one of:
  namenode -format     format the DFS filesystem
  secondarynamenode    run the DFS secondary namenode
  namenode             run the DFS namenode
  datanode             run a DFS datanode
  dfsadmin             run a DFS admin client
  fsck                 run a DFS filesystem checking utility
  fs                   run a generic filesystem user client
  balancer             run a cluster balancing utility
  jobtracker           run the MapReduce job Tracker node
  pipes                run a Pipes job
  tasktracker          run a MapReduce task Tracker node
  job                  manipulate MapReduce jobs
  queue                get information regarding JobQueues
  version              print the version
  jar <jar>            run a jar file
  distcp <srcurl> <desturl> copy file or directories recursively
  archive -archiveName NAME <src>* <dest> create a hadoop archive
  daemonlog            get/set the log level for each daemon
 or
  CLASSNAME            run the class named CLASSNAME
  
  http://developer.yahoo.com/hadoop/tutorial/module2.html#mapreduce
	 */
	
	public static void main(String args[]) throws IOException{
		Configuration cfg=new Configuration();
		FileSystem hdfs =FileSystem.get(cfg);
		Path src=new Path("/home/grid/hbaseocean/oceantext.txt");
		Path des=new Path("/");
		
		InputStream in=null;
		try {
			in=new URL("/home/grid/hbaseocean/oceantext.txt").openStream();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(in!=null)IOUtils.closeStream(in);
		}
		
		hdfs.copyFromLocalFile(src, des);
		
		FSDataOutputStream  fof=hdfs.create(new Path("/home/grid/ocean/temp"));
		
		//查询hdfs
		FileStatus fs=hdfs.getFileStatus(new Path("/home/grid/oceanup"));
		
		hdfs.rename(new Path("/home/grid/oceanup"), new Path("/home/grid/oceanup1"));
		
		hdfs.delete(new Path("/home/grid/oceanup1"), true);//是否递归
		
		BlockLocation[] bls=hdfs.getFileBlockLocations(fs, 0, fs.getLen());//块的节点位置
		
//		hdfs.copyFromLocalFile(src, dst)
		for(BlockLocation bl:bls){
			String[] hosts=bl.getHosts();
			System.out.println(hosts[0]);
		}
		
		System.out.println("fs="+new Date(fs.getModificationTime()));
		System.out.println("up to="+cfg.get("fs.default.name"));
		
	}
}
