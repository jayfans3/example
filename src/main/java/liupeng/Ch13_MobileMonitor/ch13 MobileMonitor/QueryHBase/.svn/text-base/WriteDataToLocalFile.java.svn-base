package HBaseIndexAndQuery.QueryHBase;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.hbase.util.Bytes;

import cn.cstor.cloud.hbase.cdr.CDRBase.WriteObject;

public class WriteDataToLocalFile  implements Runnable {
	private FileOutputStream out = null;
	private String localFileName = null;
	private long maxNumber = 0;
	private  BlockingQueue queue = null;;
	private boolean threadExit = false;
	
	public  WriteDataToLocalFile(String fileName, BlockingQueue lqueue)
	{

		localFileName = fileName;
		queue = lqueue;
		try {
			out = new FileOutputStream(new File(fileName));
			byte[] pad = new byte[4];
			for (int k = 0; k < 4; k++) {
				pad[k] = 0;
			}
			out.write(pad);
			out.write(pad);
			out.write(pad);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void run()
	{
		 System.out.println("start write into The local File ****************************************");
		int count = 0 ;
		boolean exit = false;
		
		while(!exit && !threadExit)
		{
		
			ArrayList<WriteObject>  list = new ArrayList<WriteObject>();
		    queue.drainTo(list);
		    for(int i = 0 ; i < list.size() ; i++)
		    {
		    	list.get(i).Write(out);
		    }
		    count = count + list.size();
		  
		    if(count == maxNumber && maxNumber != 0)
		    {
		    	exit = true;
		    }
		    if(list.size() == 0)
		    {
		    	try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		    }
		}
		System.out.println("write to file  is over " + count + "  maxNumber "  +  maxNumber);
		Close();
		SetFileOver();
	}
	
	public void SetMaxNumber( long number )
	{
		System.out.println("maxNumber is " + number);
		maxNumber = number;
		if(maxNumber == 0)
		{
			threadExit = true;
		}
	}
	private void SetFileOver()
	{
		try {
			RandomAccessFile raf = new RandomAccessFile(localFileName, "rw");
			int reslut = 1;
			raf.write(Bytes.toBytes(reslut));
			raf.write(Bytes.toBytes(maxNumber));
			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
		
	private void Close()
	{

		try {
			out.close();
			SetFileOver();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	

}
