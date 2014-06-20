package CreateIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import cn.cstor.cloud.hbase.cdr.CDRBase.CDRTable;

import MyXMLReader.MyXmlReader;


public class RepairThread  implements Runnable ,Watcher{
	
	private String connectionString = "";
	private int timeout=10000;
	private String rootPath ="";
	private String severSaveAtrrPath="";
	private  ZooKeeper  zk =null;
	private String cut="@";
	private int  redoInterval =0;
	private String hdfsURL ="";
	private FileSystem fs = null;
	private String crteateIndexConf ="";
	private String fileLog = "";
	
	public  RepairThread(String conFileName)
	{
			initConf(conFileName);
			try {
				zk = new ZooKeeper(connectionString,timeout,this);
				Configuration conf = new Configuration();
				conf.set("fs.default.name",hdfsURL);
				fs = FileSystem.get(conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
	}
	public void  setCreateIndexConf( String confFile)
	{
		crteateIndexConf = confFile;
	}
	
	public void setFileSystem( FileSystem lfs)
	{
		fs = lfs;
	}
	

	
	private void  initConf(String conFileName)
	{
    	MyXmlReader reader = new MyXmlReader(conFileName);
    	connectionString= reader.getName("connectString");
    	timeout =Integer.parseInt(reader.getName("sessionTimeout"));
        rootPath =   reader.getName("sourceRoot");
        severSaveAtrrPath = reader.getName("app") +"/" + reader.getName("serverCon");
        redoInterval =Integer.parseInt(reader.getName("redoInterval"))*60*1000;
        hdfsURL = reader.getName("hdfsURL");
	}
	
	private String getFileLogPath()
	{
		MyXmlReader reader = new MyXmlReader(crteateIndexConf);
		String tmp = reader.getName("fileLog");
		return tmp;
	}
	public void  run()
	{
			fileLog =  getFileLogPath();
			while(true)
			{
				try {
						Thread.sleep(redoInterval);
				} catch (InterruptedException e) {
						e.printStackTrace();
					}
				long time = getServerTimeAttr();
				  ArrayList<String>  files = null;
				  files = getRemainFile(time);
				  if(files != null)
				  {
					  for( int i = 0 ; i < files.size(); i++)
					  {
						  System.out.println("gyy: repair remain file is " + files.get(i));
						  fileCreateIndex(files.get(i));
					  }
				  }
			}
	}
	
	// the file which filemodifyTime < time
	private  ArrayList<String> getRemainFile(long  time)
	{
    	String parentName = rootPath;
    	ArrayList<String> fileNameList = new ArrayList<String>();
    	 try {
    		 System.out.println(parentName+"******************************");
    		 if( fs  != null)
    		 {
				FileStatus fileList[] = fs.listStatus(new Path(parentName));
				if(fileList  != null)
				{							
								for (int i = 0; i < fileList.length; i++) {
									if (time > fileList[i].getModificationTime()) {
										fileNameList.add(fileList[i].getPath().getName());
									}
								}
					}
			}
    	 } catch (IOException e) {
 			e.printStackTrace();
 			return fileNameList;
 		}
		return fileNameList;
	}
	
	private void  fileCreateIndex(String fileName)
	{
		ArrayList<Pair<String,Long>> lists = new ArrayList<Pair<String,Long>>();
		ArrayList<Pair<String,Long>> events = new ArrayList<Pair<String,Long>>();
		CDRTable.CDR type = null;
		if (fileName.matches(".+/bssap.*")) {
			if(  ! fileName.matches(".+/bssapEvent.*") )
			{
				lists.add(new Pair(fileName,0));
				type = CDRTable.CDR.BSSAP;
			}
			else
			{
				events.add(new Pair(fileName,0));
			}
	} else if (fileName.matches(".+/bicc.*")) {

		if(  ! fileName.matches(".+/biccEvent.*") )
		{
			lists.add(new Pair(fileName,0));
			type = CDRTable.CDR.BICC;
		}
		else
		{
			events.add(new Pair(fileName,0));
		}
	} else if (fileName.matches(".+/iucs.*")) {
		
		if(  ! fileName.matches(".+/iucsEvent.*") )
		{
			lists.add(new Pair(fileName,0));
			type = CDRTable.CDR.IUCS;
		}
		else
		{
			events.add(new Pair(fileName,0));
		}
	}
			CreateHDFSIndexImp bssap = new CreateHDFSIndexImp();
			bssap.setGenerateConfFile(crteateIndexConf);
			bssap.setCDRTableType(type);
			bssap.setFileList(lists);
			bssap.setLogFile(fileLog);
			bssap.setFileSystem(fs);
			bssap.createIndex();
	}
	
	private long  getServerTimeAttr()
	{
		 try {
			byte[] byteAttribute =zk.getData(severSaveAtrrPath, false,null);
			String content = Bytes.toString(byteAttribute);
			StringTokenizer as =  new  StringTokenizer(content,cut);
			String timeValueString = as.nextToken();
			System.out.println("time is " + timeValueString);
			return Long.parseLong(timeValueString);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return 0;
	}
	
    public void process(WatchedEvent event) {

    }
	
/*	public static void main(String args[])
	{
		RepairThread a = new RepairThread(args[0]);
		Thread thread  = new Thread(a);
		thread.start();
	}*/

	
}
