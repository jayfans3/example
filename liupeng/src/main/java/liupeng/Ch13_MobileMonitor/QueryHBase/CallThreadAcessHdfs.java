package HBaseIndexAndQuery.QueryHBase;

import java.util.List;

import cn.cstor.cloud.hbase.cdr.CDRBase.WriteObject;

public interface CallThreadAcessHdfs {
	public void SetThreadOver();
	public void AddWriteList(WriteObject wo);
	public WriteObject   GetCDR();
	public  void AddToFinalResultList( List<CDRPosInFile> majResultList) ;
}
