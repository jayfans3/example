import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Read2 {
	public static void main(String [] args)throws Exception{
		Configuration conf=new Configuration();
		FileSystem hdfs=FileSystem.get(conf);
		FileSystem local=FileSystem.getLocal(conf);
		
		Path inputDir=new Path("C:\\Users\\Administrator\\Desktop\\copy");
		Path hdfsFile=new Path("/testfile/");
		//System.out.println(inputDir.toString());
		hdfs.mkdirs(hdfsFile);
		
		FileStatus[] inputFiles=local.listStatus(inputDir);
		FSDataOutputStream out;
		
		for(int i=0;i<inputFiles.length;i++){
			System.out.println(inputFiles[i].getPath().getName());
			FSDataInputStream in=
				local.open(inputFiles[i].getPath());
			out=hdfs.create(new Path("/testfile/"+inputFiles[i].getPath().getName()));
			byte buffer[]=new byte[256];
			int bytesRead=0;
			while((bytesRead=in.read(buffer))>0){
				out.write(buffer,0,bytesRead);
			}
			out.close();
			in.close();
			File file = new File(inputFiles[i].getPath().toString());
			file.delete();
		}
		
		
		
	}

}
