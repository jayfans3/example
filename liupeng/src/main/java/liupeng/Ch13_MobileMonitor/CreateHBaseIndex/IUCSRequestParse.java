package HBaseIndexAndQuery.CreateHBaseIndex;



import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import Format.BytesToString;
import HBaseIndexAndQuery.HBaseDao.HBaseDaoImp;

public class IUCSRequestParse implements RequestParse {

	HBaseDaoImp dao;
	List<Put> putDataList = new ArrayList<Put>();
	List<Put> putCallingIndexItemList = new ArrayList<Put>();
	List<Put> putCalledIndexItemList = new ArrayList<Put>();
	byte[] keyFamily = new String("key").getBytes();
	String tableName = new String("hbase_iucs_cdr");
	String calledIndexTableName = new String("hbase_iucs_cdr_CalledIndex");
	String callingIndexTableName = new String("hbase_iucs_cdr_CallingIndex");
	HTable hIUCStable = null;
	HTable hIUCStableCalledIndex = null;
	HTable hIUCStableCallingIndex = null;

	byte[] rowid = new byte[20];
	byte[] start_time_s = new byte[4];
	byte[] start_time_ns = new byte[4];
	byte[] end_time_s = new byte[4];
	byte[] end_time_ns = new byte[4];
	byte[] cdr_index = new byte[4];
	byte[] cdr_type = new byte[1];
	byte[] cdr_result = new byte[1];
	byte[] base_cdr_index = new byte[4];
	byte[] base_cdr_type = new byte[1];
	byte[] tmsi = new byte[4];
	byte[] new_tmsi = new byte[4];
	byte[] imsi = new byte[8];
	byte[] imei = new byte[8];
	byte[] calling_number = new byte[8];
	byte[] called_number = new byte[8];
	byte[] third_number = new byte[24];
	byte[] mgw_ip = new byte[4];
	byte[] msc_server_ip = new byte[4];
	byte[] rnc_spc = new byte[4]; // 和BSSAP不同1
	byte[] msc_spc = new byte[4];
	byte[] lac = new byte[2];
	byte[] ci = new byte[2];
	byte[] last_lac = new byte[2];
	byte[] last_ci = new byte[2];
	byte[] cref_cause = new byte[1];
	byte[] cm_rej_cause = new byte[1];
	byte[] lu_rej_cause = new byte[1];
	byte[] assign_failure_cause = new byte[1];
	byte[] rr_cause = new byte[1];
	byte[] cip_rej_cause = new byte[1];
	byte[] disconnect_cause = new byte[1];
	byte[] cc_rel_cause = new byte[1];
	byte[] clear_cause = new byte[1];
	byte[] cp_cause = new byte[1];
	byte[] rp_cause = new byte[1];
	byte[] ho_cause = new byte[24];
	byte[] ho_failure_cause = new byte[24];
	byte[] rab_ass_failure_cause = new byte[2]; // 和BSSAP不同之2开始
	byte[] rab_rel_failure_cause = new byte[2];
	byte[] rab_rel_request_cause = new byte[2];
	byte[] iu_rel_request_cause = new byte[2];
	byte[] iu_rel_command_cause = new byte[2];
	byte[] first_paging_time = new byte[4];
	byte[] second_paging_time = new byte[4];
	byte[] third_paging_time = new byte[4];
	byte[] fourth_paging_time = new byte[4];
	byte[] cc_time = new byte[4];
	byte[] rab_ass_time = new byte[4];
	byte[] rab_ass_complete_time = new byte[4];
	byte[] setup_time = new byte[4];
	byte[] alert_time = new byte[4];
	byte[] connect_time = new byte[4];
	byte[] disconnect_time = new byte[4];
	byte[] iu_release_request_time = new byte[4];
	byte[] iu_release_command_time = new byte[4];
	byte[] rp_data_time = new byte[4];
	byte[] rp_ack_time = new byte[4];
	byte[] auth_request_time = new byte[4];
	byte[] auth_response_time = new byte[4];
	byte[] sec_mode_cmd_time = new byte[4];
	byte[] sec_mode_cmp_time = new byte[4];
	byte[] cm_service_accept_time = new byte[4];
	byte[] call_confirm_preceding_time = new byte[4];
	byte[] connect_ack_time = new byte[4];
	byte[] rab_release_request_time = new byte[4];
	byte[] relocation_request_time = new byte[4];
	byte[] relocation_request_ack_time = new byte[4];
	byte[] relocation_command_time = new byte[4];
	byte[] relocation_complete_time = new byte[4];
	byte[] relocation_detect_time = new byte[4];
	byte[] forward_srns_context_time = new byte[4];
	byte[] smsc = new byte[24];
	byte[] sm_type = new byte[1];
	byte[] sm_data_coding_scheme = new byte[1];
	byte[] sm_length = new byte[2];
	byte[] rp_data_count = new byte[1];
	byte[] handover_count = new byte[1];
	byte[] info_trans_capability = new byte[1];
	byte[] speech_version = new byte[1];
	byte[] failed_handover_count = new byte[1];
	byte[] sub_cdr_index_set = new byte[200];//
	byte[] sub_cdr_starttime_set = new byte[200];//
	byte[] call_stop = new byte[1];
	byte[] interrnc_ho_count = new byte[1];
	byte[] iu_release_cmp_time = new byte[4];
	byte[] max_bitrate = new byte[4];
	byte[] rab_release_dir = new byte[1];
	byte[] bscid = new byte[8]; // BSSAP相同之开始
	byte[] call_stop_msg = new byte[1];
	byte[] call_stop_cause = new byte[1];
	byte[] mscid = new byte[4];
	byte[] last_bscid = new byte[8];
	byte[] last_mscid = new byte[4];
	byte[] dtmf = new byte[1];
	byte[] cid = new byte[4];
	byte[] last_cid = new byte[4];
	byte[] tac = new byte[4];
	byte[] cdr_rel_type = new byte[1];
	byte[] encodedRecord = new byte[67];

	public IUCSRequestParse(HBaseDaoImp hDao) throws IOException {
		dao = hDao;
		this.hIUCStable = dao.getHBaseTable(this.tableName);
		this.hIUCStableCalledIndex = dao
				.getHBaseTable(this.calledIndexTableName);
		this.hIUCStableCallingIndex = dao
				.getHBaseTable(this.callingIndexTableName);
	}

	public boolean ParseAndInsert(long fileNameID, long fileOffset,
			FSDataInputStream fs, int singleCdrSize) {

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

			fs.read(this.start_time_s, 0, 4);// 常用字段
			fs.read(this.start_time_ns, 0, 4);
			fs.read(this.end_time_s, 0, 4);// 常用字段
			fs.read(this.end_time_ns, 0, 4);
			fs.read(this.cdr_index, 0, 4);
			fs.read(this.cdr_type, 0, 1);// 常用字段
			fs.read(this.cdr_result, 0, 1);// 常用字段

			fs.read(this.base_cdr_index, 0, 4);
			fs.read(this.base_cdr_type, 0, 1);
			fs.read(this.tmsi, 0, 4);
			fs.read(this.new_tmsi, 0, 4);
			fs.read(this.imsi, 0, 8);// 常用字段
			fs.read(this.imei, 0, 8);// 常用字段
			fs.read(this.calling_number, 0, 8);// 常用字段
			fs.read(this.called_number, 0, 8);// 常用字段
			fs.read(this.third_number, 0, 24);
			fs.read(this.mgw_ip, 0, 4);
			fs.read(this.msc_server_ip, 0, 4);
			fs.read(this.rnc_spc, 0, 4);
			fs.read(this.msc_spc, 0, 4);

			fs.read(this.lac, 0, 2);
			fs.read(this.ci, 0, 2);
			fs.read(this.last_lac, 0, 2);
			fs.read(this.last_ci, 0, 2);
			fs.read(this.cref_cause, 0, 1);
			fs.read(this.cm_rej_cause, 0, 1);
			fs.read(this.lu_rej_cause, 0, 1);
			fs.read(this.assign_failure_cause, 0, 1);
			fs.read(this.rr_cause, 0, 1);
			fs.read(this.cip_rej_cause, 0, 1);
			fs.read(this.disconnect_cause, 0, 1);
			fs.read(this.cc_rel_cause, 0, 1);
			fs.read(this.clear_cause, 0, 1);
			fs.read(this.cp_cause, 0, 1);
			fs.read(this.rp_cause, 0, 1);
			fs.read(this.ho_cause, 0, 24);
			fs.read(this.ho_failure_cause, 0, 24);
			fs.read(this.rab_ass_failure_cause, 0, 2);
			fs.read(this.rab_rel_failure_cause, 0, 2);
			fs.read(this.rab_rel_request_cause, 0, 2);

			fs.read(this.iu_rel_request_cause, 0, 2);
			fs.read(this.iu_rel_command_cause, 0, 2);
			fs.read(this.first_paging_time, 0, 4);
			fs.read(this.second_paging_time, 0, 4);
			fs.read(this.third_paging_time, 0, 4);
			fs.read(this.fourth_paging_time, 0, 4);
			fs.read(this.cc_time, 0, 4);
			fs.read(this.rab_ass_time, 0, 4);
			fs.read(this.rab_ass_complete_time, 0, 4);
			fs.read(this.setup_time, 0, 4);
			fs.read(this.alert_time, 0, 4);
			fs.read(this.connect_time, 0, 4);
			fs.read(this.disconnect_time, 0, 4);
			fs.read(this.iu_release_request_time, 0, 4);
			fs.read(this.iu_release_command_time, 0, 4);
			fs.read(this.rp_data_time, 0, 4);
			fs.read(this.rp_ack_time, 0, 4);
			fs.read(this.auth_request_time, 0, 4);
			fs.read(this.auth_response_time, 0, 4);
			fs.read(this.sec_mode_cmd_time, 0, 4);

			fs.read(this.sec_mode_cmp_time, 0, 4);
			fs.read(this.cm_service_accept_time, 0, 4);
			fs.read(this.call_confirm_preceding_time, 0, 4);
			fs.read(this.connect_ack_time, 0, 4);
			fs.read(this.rab_release_request_time, 0, 4);
			fs.read(this.relocation_request_time, 0, 4);
			fs.read(this.relocation_request_ack_time, 0, 4);
			fs.read(this.relocation_command_time, 0, 4);
			fs.read(this.relocation_complete_time, 0, 4);
			fs.read(this.relocation_detect_time, 0, 4);
			fs.read(this.forward_srns_context_time, 0, 4);
			fs.read(this.smsc, 0, 24);
			fs.read(this.sm_type, 0, 1);
			fs.read(this.sm_data_coding_scheme, 0, 1);
			fs.read(this.sm_length, 0, 2);
			fs.read(this.rp_data_count, 0, 1);
			fs.read(this.handover_count, 0, 1);
			fs.read(this.info_trans_capability, 0, 1);
			fs.read(this.speech_version, 0, 1);
			fs.read(this.failed_handover_count, 0, 1);

			fs.read(this.sub_cdr_index_set, 0, 200);
			fs.read(this.sub_cdr_starttime_set, 0, 200);
			fs.read(this.call_stop, 0, 1);
			fs.read(this.interrnc_ho_count, 0, 1);
			fs.read(this.iu_release_cmp_time, 0, 4);
			fs.read(this.max_bitrate, 0, 4);
			fs.read(this.rab_release_dir, 0, 1);
			fs.read(this.bscid, 0, 8);// 常用字段
			fs.read(this.call_stop_msg, 0, 1);
			fs.read(this.call_stop_cause, 0, 1);
			fs.read(this.mscid, 0, 4);// 常用字段
			fs.read(this.last_bscid, 0, 8);// 常用字段
			fs.read(this.last_mscid, 0, 4);// 常用字段
			fs.read(this.dtmf, 0, 1);
			fs.read(this.cid, 0, 4);
			fs.read(this.last_cid, 0, 4);
			fs.read(this.tac, 0, 4);
			fs.read(this.cdr_rel_type, 0, 1);// 常用字段

			EncodeCdrForMajorTable();
			p.add(this.keyFamily, null, BytesToString.Bytes8tolong(bscid),
					encodedRecord);
			this.putDataList.add(p);
			/*
			 * index tables needed are call num index; time index; and cell
			 * index; and all kinds of KPI related index;
			 */
			ConstructCallingNumIndexItem(rowKey);
			ConstructCalledNumIndexItem(rowKey);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	public void EncodeCdrForMajorTable() {
		System.arraycopy(start_time_s, 0, encodedRecord, 0, 4);
		System.arraycopy(end_time_s, 0, encodedRecord, 4, 4);
		System.arraycopy(cdr_type, 0, encodedRecord, 8, 1);
		System.arraycopy(cdr_result, 0, encodedRecord, 9, 1);
		System.arraycopy(imsi, 0, encodedRecord, 10, 8);
		System.arraycopy(imei, 0, encodedRecord, 18, 8);
		System.arraycopy(calling_number, 0, encodedRecord, 26, 8);
		System.arraycopy(called_number, 0, encodedRecord, 34, 8);
		System.arraycopy(bscid, 0, encodedRecord, 42, 8);
		System.arraycopy(mscid, 0, encodedRecord, 50, 4);
		System.arraycopy(last_bscid, 0, encodedRecord, 54, 8);
		System.arraycopy(last_mscid, 0, encodedRecord, 62, 4);
		System.arraycopy(cdr_rel_type, 0, encodedRecord, 66, 1);
	}

	private void ConstructCallingNumIndexItem(byte[] rowKey) {
		// calling num + start_time_s
		int i = 0;
		for (i = 0; i < 8; i++) {
			if (calling_number[i] != 0) {
				break;
			}
		}
		if (8 == i) {
			return;
		}
		byte[] callingNumTime = new byte[25];

		System.arraycopy(calling_number, 0, callingNumTime, 0, 8);
		System.arraycopy(start_time_s, 0, callingNumTime, 8, 4);
		System.arraycopy(cdr_type, 0, callingNumTime, 12, 1);
		System.arraycopy(cdr_index, 0, callingNumTime, 13, 4);
		System.arraycopy(mscid, 0, callingNumTime, 17, 4);
		System.arraycopy(cid, 0, callingNumTime, 21, 4);

		Put p = new Put(callingNumTime);
		p.add(Bytes.toBytes("Offset"), null, BytesToString.Bytes8tolong(bscid),
				rowKey);
		putCallingIndexItemList.add(p);
	}

	private void ConstructCalledNumIndexItem(byte[] rowKey) {
		int i = 0;
		for (i = 0; i < 8; i++) {
			if (called_number[i] != 0) {
				break;
			}
		}
		if (8 == i) {
			return;
		}

		byte[] calledNumTime = new byte[25];
		System.arraycopy(called_number, 0, calledNumTime, 0, 8);
		System.arraycopy(start_time_s, 0, calledNumTime, 8, 4);
		System.arraycopy(cdr_type, 0, calledNumTime, 12, 1);
		System.arraycopy(cdr_index, 0, calledNumTime, 13, 4);
		System.arraycopy(mscid, 0, calledNumTime, 17, 4);
		System.arraycopy(cid, 0, calledNumTime, 21, 4);

		Put p = new Put(calledNumTime);
		p.add(Bytes.toBytes("Offset"), null, BytesToString.Bytes8tolong(bscid),
				rowKey);
		putCalledIndexItemList.add(p);
	}

	public void Clear() {
		this.putDataList.clear();
		this.putCalledIndexItemList.clear();
		this.putCallingIndexItemList.clear();
	}

	public void Commit() {
		try {
			if (this.putDataList.isEmpty() == false) {
				System.out.println("insert to hbase_iucs_cdr");
				this.hIUCStable.put(this.putDataList);
				this.putDataList.clear();
				this.hIUCStable.flushCommits();
			}

			// put called and calling index items
			if (this.putCalledIndexItemList.isEmpty() == false) {
				System.out
						.println("insert to hbase_iucs_cdr_Called_Index, item count: "
								+ this.putCalledIndexItemList.size());
				this.hIUCStableCalledIndex.put(this.putCalledIndexItemList);
				this.putCalledIndexItemList.clear();
				this.hIUCStableCalledIndex.flushCommits();
			}
			if (this.putCallingIndexItemList.isEmpty() == false) {
				System.out
						.println("insert to hbase_iucs_cdr_CallingIndex, item count: "
								+ this.putCallingIndexItemList.size());
				this.hIUCStableCallingIndex.put(this.putCallingIndexItemList);
				this.putCallingIndexItemList.clear();
				this.hIUCStableCallingIndex.flushCommits();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
