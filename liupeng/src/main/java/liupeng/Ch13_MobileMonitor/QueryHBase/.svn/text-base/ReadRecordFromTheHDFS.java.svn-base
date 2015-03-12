package HBaseIndexAndQuery.QueryHBase;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.cstor.cloud.hbase.cdr.CDRBase.WriteObject;

public class ReadRecordFromTheHDFS implements Runnable {

	public List<CDRPosInFile> list = null;
	FileSystem fs = null;
	CallThreadAcessHdfs parentThread = null;
	private  BlockingQueue queue = null;

	public ReadRecordFromTheHDFS(List<CDRPosInFile> lList,BlockingQueue lqueue, FileSystem lfs,
			CallThreadAcessHdfs lparentThread) {
		list = lList;
		fs = lfs;
		parentThread = lparentThread;
		queue = lqueue;
	}

	public void run() {
		

		Comparator comp = new CDRPosComparator();
		Collections.sort(list, comp);

		long fileID = -1;

		try {
			Iterator<CDRPosInFile> cdrPos = list.iterator();
			FSDataInputStream hdfsInStream = null;
			while (cdrPos.hasNext()) {
				WriteObject cdr = null;
				CDRPosInFile pos = cdrPos.next();
				if (fileID == -1) // 第一次读文件
				{
					fileID = pos.fileID;
					String path =  CDRPosInFile.GetFileName(fileID);
					
						hdfsInStream = fs.open(new Path(path));
						hdfsInStream.seek(pos.offset);
						// 读取记录
						cdr = ReadRecord(hdfsInStream);
					
				} else if (fileID == pos.fileID) // 如果记录在相同的文件内
				{
//						System.out.println("Offset is " + pos.offset +" pos  is " +  hdfsInStream.getPos() +" path is " + CDRPosInFile.GetFileName(fileID) +"  " + Thread.currentThread());
						hdfsInStream.seek(pos.offset - hdfsInStream.getPos());
						// 读取记录
						cdr = ReadRecord(hdfsInStream);
					
				} else if (fileID != pos.fileID) // 如果记录不在相同的文件内
				{
					hdfsInStream.close();
					fileID = pos.fileID;
					String path =  CDRPosInFile.GetFileName(fileID);
					hdfsInStream = fs.open(new Path(path));
					hdfsInStream.seek(pos.offset);
					// 读取记录
					cdr = ReadRecord(hdfsInStream);
				}

				if (cdr != null) {
					if (queue.remainingCapacity() != 0) {
						queue.put(cdr);
						synchronized (queue){
								queue.notify();
						}
					}
				parentThread.AddWriteList(cdr);
				}
			}
			
			if(hdfsInStream != null)
			{
				hdfsInStream.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			parentThread.SetThreadOver();

		}
	}

	public WriteObject ReadRecord(FSDataInputStream stream) {
		WriteObject cdr = parentThread.GetCDR();
		cdr.ReadCDRFromHDFS(stream);
		return cdr;
	}

}
