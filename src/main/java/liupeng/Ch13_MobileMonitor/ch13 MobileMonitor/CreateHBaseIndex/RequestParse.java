package HBaseIndexAndQuery.CreateHBaseIndex;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
public interface RequestParse {
	
	public boolean ParseAndInsert(long fileNameID, long fileOffset, FSDataInputStream fs, int singleCdrSize );
	public void Commit();

}
