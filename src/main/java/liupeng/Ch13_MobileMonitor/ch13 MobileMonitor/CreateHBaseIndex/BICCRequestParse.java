package HBaseIndexAndQuery.CreateHBaseIndex;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import HBaseIndexAndQuery.HBaseDao.HBaseDaoImp;

public class BICCRequestParse implements RequestParse {

	HBaseDaoImp dao;
	List<Put> putDataList = new ArrayList<Put>();
	byte[] keyFamily = new String("key").getBytes();
	String tableName = new String("hbase_bicc_cdr");
	String calledIndexTableName = new String("hbase_bicc_cdr_CalledIndex");
	String callingIndexTableName = new String("hbase_bicc_cdr_CallingIndex");
	HTable hBICCtable = null;
	HTable hBICCtableCalledIndex = null;
	HTable hBICCtableCallingIndex = null;
	List<Put> putCallingIndexItemList = new ArrayList<Put>();
	List<Put> putCalledIndexItemList = new ArrayList<Put>();

	byte[] start_time_s = new byte[4];
	byte[] start_time_ns = new byte[4];
	byte[] end_time_s = new byte[4];
	byte[] end_time_ns = new byte[4];
	byte[] cdr_index = new byte[4];
	byte[] source_ip = new byte[4];
	byte[] destination_ip = new byte[4];
	byte[] cic = new byte[4];
	byte[] opc = new byte[4];
	byte[] dpc = new byte[4];
	byte[] release_reason = new byte[1];
	byte[] calling_party_number = new byte[8];
	byte[] called_party_number = new byte[24];
	byte[] original_called_number = new byte[24];
	byte[] transfer_number = new byte[24];
	byte[] location_number = new byte[24];
	byte[] response_time = new byte[4];
	byte[] acm_time = new byte[4];
	byte[] anm_time = new byte[4];
	byte[] rel_time = new byte[4];
	byte[] call_duration = new byte[4];
	byte[] duration = new byte[4];
	byte[] codec_modify_flag = new byte[1];
	byte[] codec_modify_result = new byte[1];
	byte[] codec_negotiation_flag = new byte[1];
	byte[] codec_negotiation_result = new byte[1];
	byte[] codec_type = new byte[4];
	byte[] call_type = new byte[1];
	byte[] call_hold = new byte[1];
	byte[] call_forward = new byte[1];
	byte[] call_waiting = new byte[1];
	byte[] confrence_call = new byte[1];
	byte[] is_ext_platform = new byte[1];
	byte[] encodedRecord = new byte[53];

	public BICCRequestParse(HBaseDaoImp ldao) throws IOException {
		dao = ldao;
		this.hBICCtable = dao.getHBaseTable(this.tableName);
		this.hBICCtableCalledIndex = dao
				.getHBaseTable(this.calledIndexTableName);
		this.hBICCtableCallingIndex = dao
				.getHBaseTable(this.callingIndexTableName);
	}

	@SuppressWarnings("finally")
	public boolean ParseAndInsert(long fileNameID, long fileOffset,
			FSDataInputStream fs, int singleCdrSize) {
		boolean reslut = false;
		try {


			int rowKeyLength =8 + 8;
			byte[] rowKey = new byte[rowKeyLength];

			int i = 0;
			
			for (i = 0; i < 8; i++) {
				rowKey[i] = (byte) ((fileNameID >> (7 - i) * 8) & 0xFF);
			}

			for (i = 0; i < 8; i++) {
				rowKey[8 + i] = (byte) ((fileOffset >> (7 - i) * 8) & 0xFF);
			}

			Put p = new Put(rowKey);

			fs.read(this.start_time_s, 0, 4); // 常用字段
			fs.read(this.start_time_ns, 0, 4);
			fs.read(this.end_time_s, 0, 4);// 常用字段
			fs.read(this.end_time_ns, 0, 4);
			fs.read(this.cdr_index, 0, 4);// 常用字段
			fs.read(this.source_ip, 0, 4);
			fs.read(this.destination_ip, 0, 4);
			fs.read(this.cic, 0, 4);
			fs.read(this.opc, 0, 4);// 常用字段
			fs.read(this.dpc, 0, 4);// 常用字段

			fs.read(this.release_reason, 0, 1);
			fs.read(this.calling_party_number, 0, 8);// 常用字段
			fs.read(this.called_party_number, 0, 24);// 常用字段
			fs.read(this.original_called_number, 0, 24);
			fs.read(this.transfer_number, 0, 24);
			fs.read(this.location_number, 0, 24);
			fs.read(this.response_time, 0, 4);
			fs.read(this.acm_time, 0, 4);
			fs.read(this.anm_time, 0, 4);
			fs.read(this.rel_time, 0, 4);

			fs.read(this.call_duration, 0, 4);
			fs.read(this.duration, 0, 4);
			fs.read(this.codec_modify_flag, 0, 1);
			fs.read(this.codec_modify_result, 0, 1);
			fs.read(this.codec_negotiation_flag, 0, 1);
			fs.read(this.codec_negotiation_result, 0, 1);
			fs.read(this.codec_type, 0, 4);
			fs.read(this.call_type, 0, 1);// 常用字段
			fs.read(this.call_hold, 0, 1);
			fs.read(this.call_forward, 0, 1);
			fs.read(this.call_waiting, 0, 1);
			fs.read(this.confrence_call, 0, 1);
			fs.read(this.is_ext_platform, 0, 1);

			EncodeCdrForMajorTable();
			p.add(keyFamily, null, encodedRecord);
			this.putDataList.add(p);

			ConstructCallingNumIndexItem(rowKey);
			ConstructCalledNumIndexItem(rowKey);

			reslut = true;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			return reslut;
		}

	}

	private void ConstructCalledNumIndexItem(byte[] rowKey) {
		int i = 0;
		for (i = 0; i < 8; i++) {
			if (called_party_number[i] != 0) {
				break;
			}
		}
		if (8 == i) {
			return;
		}
		// 建立索引的ROWKEY
		byte[] calledNumTime = new byte[40];
		System.arraycopy(called_party_number, 0, calledNumTime, 0, 24);
		System.arraycopy(start_time_s, 0, calledNumTime, 24, 4);
		System.arraycopy(opc, 0, calledNumTime, 28, 4);
		System.arraycopy(dpc, 0, calledNumTime, 32, 4);
		System.arraycopy(cdr_index, 0, calledNumTime, 36, 4);

		Put p = new Put(calledNumTime);
		p.add(Bytes.toBytes("Offset"), null, rowKey);
		putCalledIndexItemList.add(p);
	}

	private void ConstructCallingNumIndexItem(byte[] rowKey) {
		// calling num + start_time_s
		int i = 0;
		for (i = 0; i < 8; i++) {
			if (calling_party_number[i] != 0) {
				break;
			}
		}
		if (8 == i) {
			return;
		}
		// 建立索引的ROWKEY
		byte[] callingNumTimeOpcDpc = new byte[24];
		System.arraycopy(calling_party_number, 0, callingNumTimeOpcDpc, 0, 8);
		System.arraycopy(start_time_s, 0, callingNumTimeOpcDpc, 8, 4);
		System.arraycopy(opc, 0, callingNumTimeOpcDpc, 12, 4);
		System.arraycopy(dpc, 0, callingNumTimeOpcDpc, 16, 4);
		System.arraycopy(cdr_index, 0, callingNumTimeOpcDpc, 20, 4);

		Put p = new Put(callingNumTimeOpcDpc);
		p.add(Bytes.toBytes("Offset"), null, rowKey);
		putCallingIndexItemList.add(p);
	}

	// 主表多字段拼接为一列
	public void EncodeCdrForMajorTable() {
		System.arraycopy(start_time_s, 0, encodedRecord, 0, 4);
		System.arraycopy(end_time_s, 0, encodedRecord, 4, 4);
		System.arraycopy(cdr_index, 0, encodedRecord, 8, 4);
		System.arraycopy(opc, 0, encodedRecord, 12, 4);
		System.arraycopy(dpc, 0, encodedRecord, 16, 4);
		System.arraycopy(calling_party_number, 0, encodedRecord, 20, 8);
		System.arraycopy(called_party_number, 0, encodedRecord, 28, 24);
		System.arraycopy(call_type, 0, encodedRecord, 52, 1);
	}

	public void Clear() {
		this.putDataList.clear();
		this.putCalledIndexItemList.clear();
		this.putCallingIndexItemList.clear();
	}

	public void Commit() {
		try {
			if (this.putDataList.isEmpty() == false) {
				System.out.println("insert to hbase_bicc_cdr");
				this.hBICCtable.put(this.putDataList);
				this.putDataList.clear();
				this.hBICCtable.flushCommits();
			}

			// put called and calling index items
			if (this.putCalledIndexItemList.isEmpty() == false) {
				System.out
						.println("insert to hbase_bicc_cdr_Called_Index, item count: "
								+ this.putCalledIndexItemList.size());
				this.hBICCtableCalledIndex.put(this.putCalledIndexItemList);
				this.putCalledIndexItemList.clear();
				this.hBICCtableCalledIndex.flushCommits();
			}

			if (this.putCallingIndexItemList.isEmpty() == false) {
				System.out
						.println("insert to hbase_bicc_cdr_CallingIndex, item count: "
								+ this.putCallingIndexItemList.size());
				this.hBICCtableCallingIndex.put(this.putCallingIndexItemList);
				this.putCallingIndexItemList.clear();
				this.hBICCtableCallingIndex.flushCommits();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
