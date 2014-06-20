package HBaseIndexAndQuery.QueryHBase.Bssap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import HBaseIndexAndQuery.QueryHBase.CDRPosInFile;
import HBaseIndexAndQuery.QueryHBase.CallThreadAcessHdfs;
import HBaseIndexAndQuery.QueryHBase.HBaseIndex;
import HBaseIndexAndQuery.QueryHBase.QueryBase;
import HBaseIndexAndQuery.QueryHBase.ReadRecordFromTheHDFS;
import HBaseIndexAndQuery.QueryHBase.WriteDataToLocalFile;
import cn.cstor.cloud.hbase.cdr.CDRBase.BSSAPCDR;
import cn.cstor.cloud.hbase.cdr.CDRBase.WriteObject;


public class BSSAPQuery implements CallThreadAcessHdfs  , Runnable  ,QueryBase{
	/**
	 * @param args
	 */

	private long finalResultCount = 0;																	//the total number of the query;
	private BlockingQueue blockqueue = new ArrayBlockingQueue(1000);   //the data 
	private BlockingQueue writeToLocalFileBlockqueue = new ArrayBlockingQueue(1000);   //the data 
	private ExecutorService executorService;	 														//thread pool
	private FileSystem fs = null;				 																//fs to  connect the hdfs
	
	//about the  thread mangner
	private int threadNumber = 0;
	
	//the name of the file reslut;
	private String localFile = null;
	public boolean startQueryPri = false;
	
	private String timeName = null;
	 List<Pair<Integer, Integer>> timeRangeList = null;
	 String calledNum = null;
	 String callingNum = null;
	 List<Pair<Long, Long>> numRangeList = null;
	 String cdr_type = null;
	 List<Integer> cdrValList = null;
	 List<Long> mscList = null;
	 List<Long> cidList = null;
	 List<Long> bscList = null;
	private List<CDRPosInFile> majResult = null;

	private boolean isFirstThread = true;
	private int firstGetNumber = 100;
		
	private String delimet = ",";

	private boolean  chooseMsc = false;
	private boolean  chooseBsc = false;
	private boolean  chooseCid = false;
	private HBaseIndex index =null;
	


	public void ChooseIndexForQuery() {
		try {
			Configuration fsConf = new Configuration();
			fsConf.set("fs.default.name", "hdfs://192.168.1.8:9000");
			FileSystem lfs = FileSystem.get(fsConf);
			SetFileSystem(lfs);
		} catch (Exception e) {
			e.printStackTrace();
		}
		if(chooseMsc)    //选择MSC的索引
		{
				if(calledNum != null)
				{
				
				}
				else if(callingNum != null)
				{
					
				}
		}
		else if(chooseBsc)   //选择BSC的索引
		{
				if(calledNum != null)
				{
					
				}
				else if(callingNum != null)
				{
					
				}
		}
		else if(chooseCid)  //选择CID的索引
		{
				if(calledNum != null)
				{
					System.out.println("choose   BssapIndexCIDCalledIndex ");
					index = new BssapIndexCIDCalledIndex(this);
					index.SetCondtion(BssapConditionColumn.start_time, timeRangeList);
					index.SetCondtion(BssapConditionColumn.called_number, numRangeList);
					index.SetCondtion(BssapConditionColumn.cid, cidList);
					if(cdrValList.size() > 0)
					{
							index.SetCondtion(BssapConditionColumn.cdr_type, cdrValList);
					}
				
				}
				else if(callingNum != null)
				{

				}
		}
		else
		{
			if(calledNum != null)
			{
				index = new BssapIndexCalledIndex( this);
				index.SetCondtion(BssapConditionColumn.called_number, numRangeList);
				index.SetCondtion(BssapConditionColumn.start_time, timeRangeList);
				if(cdrValList != null && cdrValList.size() > 0)
				{
						index.SetCondtion(BssapConditionColumn.cdr_type, cdrValList);
				}
			}
			else if(callingNum != null)
			{
				index = new BssapIndexCallingIndex( this);
				index.SetCondtion(BssapConditionColumn.called_number, numRangeList);
				index.SetCondtion(BssapConditionColumn.start_time, timeRangeList);
				if(cdrValList != null && cdrValList.size() > 0)
				{
						index.SetCondtion(BssapConditionColumn.cdr_type, cdrValList);
				}
			}
		}
		
		index.RunQuery();

	}

	public void SetConditionRange(String valName, String start, String end) {
		if (valName.equals("calling_number")) {
			if (callingNum == null) {
				callingNum = "calling_number";
			}
			if (numRangeList == null) {
				numRangeList = new ArrayList<Pair<Long, Long>>();
			}
			numRangeList.add(new Pair<Long, Long>(Long.parseLong(start),Long.parseLong(end)));
		} else if (valName.equals("called_number")) {
			if (calledNum == null) {
				calledNum = "called_number";
			}
			if (numRangeList == null) {
				numRangeList = new ArrayList<Pair<Long, Long>>();
			}
			numRangeList.add(new Pair<Long, Long>(Long.parseLong(start),Long.parseLong(end)));
		} else 	if (valName.equals("start_time_s") || valName.equals("end_time_s")) {
			timeName = valName;
			if (timeRangeList == null) {
				timeRangeList = new ArrayList<Pair<Integer, Integer>>();
			}
			timeRangeList.add(new Pair<Integer, Integer>(
											Integer.parseInt(start), 
											Integer.parseInt(end)));
		}else 
		{
			System.out.println("Condition is not handled");
		}
	}
	
	public void SetConditionValue(String valName,String values)
	{
		if (valName.equals("cid") ) {
			if (cidList == null) {
				cidList = new ArrayList<Long>();
			}
			StringTokenizer  as = new StringTokenizer(values,delimet);
			while(as.hasMoreTokens())
			{
				String value = as.nextToken();
				cidList.add(Long.parseLong(value));
			}
				chooseCid = true;
		}else if(valName.equals("mscid")){
			if (mscList == null) {
				mscList = new ArrayList<Long>();
			}
			StringTokenizer  as = new StringTokenizer(values,delimet);
			while(as.hasMoreTokens())
			{
				String value = as.nextToken();
				mscList.add(Long.parseLong(value));
			}	
			chooseMsc = true;
		}
		else if(valName.equals("bscid")) {
				if (bscList == null) {
					bscList = new ArrayList<Long>();
				}
				StringTokenizer  as = new StringTokenizer(values,delimet);
				while(as.hasMoreTokens())
				{
					String value = as.nextToken();
					bscList.add(Long.parseLong(value));
				}
				chooseBsc = true;
		}else if (valName.equals("cdr_type")) {
			cdr_type = "cdr_type";
			if (cdrValList == null) {
				cdrValList = new ArrayList<Integer>();
			}
			StringTokenizer  as = new StringTokenizer(values,delimet);
			while(as.hasMoreTokens())
			{
				String value = as.nextToken();
				cdrValList.add(Integer.parseInt(value));
			}
		}
	}

	

	public synchronized void ThreadInc() {
		threadNumber++;
	}

	public synchronized void ThreadDesc() {
		threadNumber--;
	}

	public synchronized int GetThreadNumber() {
		return threadNumber;
	}

	public void SetFileSystem(FileSystem lfs) {
		fs = lfs;
	}

	public void Close() throws IOException {
	
		executorService.shutdown();

	}
	
	public void SetFileName(String lfileName) {
		localFile = lfileName;
		System.out.println(localFile);
	}
	
	public void AddWriteList(WriteObject wo)
	{
		wo.AddWriteList(writeToLocalFileBlockqueue);
	}
	public WriteObject   GetCDR()
	{
		return new BSSAPCDR();
	}

	public void SetThreadOver() {
		ThreadDesc();
		synchronized (this){
			notify();
		}
	}


	public ArrayList<BSSAPCDR> GetData(int number) {
		
		int count = 0;
		while(blockqueue.size() < number)
		{
			synchronized (blockqueue){
				try {
					blockqueue.wait();
				} catch (InterruptedException e) {
						e.printStackTrace();
				}
			}
			if(GetThreadNumber() == 0)
			{
				break;
			}
		}
		ArrayList<BSSAPCDR> list = new ArrayList<BSSAPCDR>();
		blockqueue.drainTo(list);
		return list;
	}
	
	
	public void AddToFinalResultList(List<CDRPosInFile> majResultList)
	{
		AddToFinalResultList(majResultList , false);
	}
	
	public synchronized void AddToFinalResultList(List<CDRPosInFile> majResultList , boolean over) {
				if (null == majResult) {
					majResult = new ArrayList<CDRPosInFile>();
				}
				if( !over)
				{
						finalResultCount += majResultList.size();
						majResult.addAll(majResultList);
						if(isFirstThread && majResult.size() > firstGetNumber)
						{
							isFirstThread = false;
							ThreadInc();
							executorService.execute(new ReadRecordFromTheHDFS(majResult,
									blockqueue, fs, this));
							majResult = new ArrayList<CDRPosInFile>();
							return ;
						}
						if (majResult.size() > 1000) {
								ThreadInc();
								executorService.execute(new ReadRecordFromTheHDFS(majResult,
											blockqueue, fs, this));
								majResult = new ArrayList<CDRPosInFile>();
						}
				}
				else  // over的时候,结果已经加入到了majReslut，但是由于不满足1000条，所以无法执行，因此需要执行一次
				{
					if(majResult != null && majResult.size() != 0)
					{
						ThreadInc();
						executorService.execute(new ReadRecordFromTheHDFS(majResult,
								blockqueue, fs, this));
					}
				}
		}



	public WriteDataToLocalFile StartWriteFileThread()
	{
		WriteDataToLocalFile wd = new WriteDataToLocalFile(localFile,writeToLocalFileBlockqueue);
		if(wd != null )
		{
			System.out.println("WriteDataToLocalFile  is OK");
		}
		Thread thread = new Thread(wd);
		thread.start();
		return wd;
	}
	
	public void ParseCDRPos(  List<byte[]> IndexResultList)
	{
/*		byte[] rowkey = null;
		for (int i = 0; i < IndexResultList.size(); i++) {
		rowkey = IndexResultList.get(i);
		if (rowkey != null) {
			long fileNameID = Bytes.toLong(rowkey, 0, 8);
			long fileOffset = Bytes.toLong(rowkey, 8,
					8);
			System.out.println("fileNameID is " + fileNameID +"      fileOffset is   " +fileOffset );
		}
	}*/
		
		List<CDRPosInFile> resultList = new ArrayList<CDRPosInFile>();
		int i = 0;
		byte[] rowkey = null;
			for (i = 0; i < IndexResultList.size(); i++) {
				rowkey = IndexResultList.get(i);
				if (rowkey != null) {
					long fileNameID = Bytes.toLong(rowkey, 0, 8);
					long fileOffset = Bytes.toLong(rowkey, 8,
							8);
					//System.out.println("fileNameID is " + fileNameID +"      fileOffset is   " +fileOffset );
					resultList.add(new CDRPosInFile(fileNameID, fileOffset));
				}
			}
			if (resultList.isEmpty() == false) {
				AddToFinalResultList(resultList);
			}
	}
	
	public void RunQuery() {
		
		WriteDataToLocalFile wd = StartWriteFileThread();
		int phoneNumberIndex = 0;
		int timeRangeIndex = 0;
		int numGetFromIndex = 1000;
		executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 20);
		try {
			ChooseIndexForQuery();
		} catch (Exception e) {
			e.printStackTrace();
		}
		AddToFinalResultList(null , true);
		//等待执行完毕
		while (GetThreadNumber() > 0){
			synchronized (this){
				try{
					this.wait();
					System.out.println(" the tolal number  " + threadNumber  );
				}catch (Exception e){
					e.printStackTrace();
				}
			}
		}
		System.out.println("End Time: "
				+ Calendar.getInstance().getTime().toString());
		
		wd.SetMaxNumber(finalResultCount);
		
		//if data is not more than 1000,then notify the getData function;
		synchronized (blockqueue){
			blockqueue.notify();
		}
		
		System.out.println("Query is Completed, " + Calendar.getInstance().getTime().toString());
	}
	
	public void run()
	{
		majResult = new ArrayList<CDRPosInFile>();
		RunQuery();
		try {
			Close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {

		BSSAPQuery bssapQuery = new BSSAPQuery();
		bssapQuery.executorService = Executors.newFixedThreadPool(Runtime
				.getRuntime().availableProcessors() * 50);
		bssapQuery.SetConditionRange("called_number", "10086",
				"10086");
		int startTime = 1201762834-60*60*24*13*20000;
		int endTime   =  1301762834;
		bssapQuery.SetConditionRange("start_time_s", Integer.toString(startTime), Integer.toString(endTime)); //46

//		bssapQuery.SetConditionValue("cdr_type", "1");
//		bssapQuery.SetConditionValue("cdr_type", "2");
//		bssapQuery.SetConditionValue("cid", "1342211948");
		bssapQuery.SetFileName("D:/" + new Date().getTime() + ".dat");
		CDRPosInFile.GetFileFromID(0, 1000);
		System.out.println("Start Time: "
				+ Calendar.getInstance().getTime().toString());
		Thread thread = new Thread(bssapQuery);
		thread.start();
		System.out.println("get first data  " +bssapQuery.GetData(100).size());
		System.out.println("end Time: "
				+ Calendar.getInstance().getTime().toString());
	
	}
}
