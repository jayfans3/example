package HBaseIndexAndQuery.CreateHBaseIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import cn.cstor.cloud.hbase.cdr.CDRBase.BSSAPCDR;

import HBaseIndexAndQuery.HBaseDao.HBaseDaoImp;
import Format.BytesToString;


public class BSSAPRequestParse implements RequestParse {

	HBaseDaoImp dao;
//	List<Put> putDataList = new ArrayList<Put>();
	List<Put> putCallingIndexItemList = new ArrayList<Put>();
	List<Put> putCalledIndexItemList = new ArrayList<Put>();
	
	List<Put> mscputCallingIndexItemList = new ArrayList<Put>();
	List<Put> mscputCalledIndexItemList = new ArrayList<Put>();
	
	List<Put> bscputCallingIndexItemList = new ArrayList<Put>();
	List<Put> bscputCalledIndexItemList = new ArrayList<Put>();
	
	
	List<Put> cidputCallingIndexItemList = new ArrayList<Put>();
	List<Put> cidputCalledIndexItemList = new ArrayList<Put>();
	
	
	byte[] keyFamily = new String("key").getBytes();
	String calledIndexTableName = new String("hbase_bssap_cdr_CalledIndex");
	String callingIndexTableName = new String("hbase_bssap_cdr_CallingIndex");
	String msccalledIndexTableName = new String("hbase_bssap_cdr_msc_CalledIndex");
	String msccallingIndexTableName = new String("hbase_bssap_cdr_msc_CallingIndex");
	String bsccalledIndexTableName = new String("hbase_bssap_cdr_bsc_CalledIndex");
	String bsccallingIndexTableName = new String("hbase_bssap_cdr_bsc_CallingIndex");
	String cidcalledIndexTableName = new String("hbase_bssap_cdr_cid_CalledIndex");
	String cidcallingIndexTableName = new String("hbase_bssap_cdr_cid_CallingIndex");
	
	HTable hBSSAPtableCalledIndex = null;
	HTable hBSSAPtableCallingIndex = null;
	
	HTable hMSCBSSAPtableCalledIndex = null;
	HTable hMSCBSSAPtableCallingIndex = null;
	
	HTable hBSCBSSAPtableCalledIndex = null;
	HTable hBSCBSSAPtableCallingIndex = null;
	
	HTable hCIDBSSAPtableCalledIndex = null;
	HTable hCIDBSSAPtableCallingIndex = null;

	//byte[] encodedRecord = new byte[67];
	byte[] rowid = new byte[8];
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
	byte[] bsc_spc = new byte[4]; 
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
	byte[] first_paging_time = new byte[4];
	byte[] second_paging_time = new byte[4];
	byte[] third_paging_time = new byte[4];
	byte[] fourth_paging_time = new byte[4];
	byte[] cc_time = new byte[4];
	byte[] assignment_time = new byte[4];
	byte[] assignment_cmp_time = new byte[4];
	byte[] setup_time = new byte[4];
	byte[] alert_time = new byte[4];
	byte[] connect_time = new byte[4];
	byte[] disconnect_time = new byte[4];
	byte[] clear_request_time = new byte[4];
	byte[] clear_command_time = new byte[4];
	byte[] rp_data_time = new byte[4];
	byte[] rp_ack_time = new byte[4];
	byte[] aut_request_time = new byte[4];
	byte[] aut_response_time = new byte[4];
	byte[] cm_service_accept_time = new byte[4];
	byte[] call_confirm_preceding_time = new byte[4];
	byte[] connect_ack_time = new byte[4];
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
	byte[] interbsc_ho_count = new byte[1];
	byte[] pcmts = new byte[2];
	byte[] bscid = new byte[8];
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
	
    byte[] last_pcm = new byte[2];
	byte[] identity_request_time  = new byte[4];
	byte[] identity_response_time = new byte[4];
	byte[] ciph_mode_cmd_time = new byte[4];
	byte[] ciph_mode_cmp_time = new byte[4];
	byte[] tmsi_realloc_cmd_time = new byte[4];
	byte[] tmsi_realloc_cmp_time = new byte[4];
	byte[] cc_release_time  = new byte[4];
	byte[] cc_release_cmp_time = new byte[4];
	byte[] clear_cmp_time = new byte[4];
	byte[] sccp_release_time = new byte[4];
	byte[] sccp_release_cmp_time = new byte[4];
	
	int size = BSSAPCDR.SIZE;

	public BSSAPRequestParse(HBaseDaoImp  ldao) throws IOException {
		dao = ldao;
//		this.hBSSAPtable = dao.getHBaseTable(this.tableName);
		this.hBSSAPtableCalledIndex = dao
				.getHBaseTable(this.calledIndexTableName);
		this.hBSSAPtableCallingIndex = dao
				.getHBaseTable(this.callingIndexTableName);
		
		 hMSCBSSAPtableCalledIndex =  dao
			.getHBaseTable(this.msccalledIndexTableName);
		 hMSCBSSAPtableCallingIndex =  dao
			.getHBaseTable(this.msccallingIndexTableName);
		
		 hBSCBSSAPtableCalledIndex = dao
			.getHBaseTable(this.bsccalledIndexTableName);
		 hBSCBSSAPtableCallingIndex =  dao
			.getHBaseTable(this.bsccallingIndexTableName);
		
		 hCIDBSSAPtableCalledIndex =  dao
			.getHBaseTable(this.cidcalledIndexTableName);
		 hCIDBSSAPtableCallingIndex =  dao
			.getHBaseTable(this.cidcallingIndexTableName);

	}

	
	
	
	
	public boolean ParseAndInsert(long fileNameID, long fileOffset,
			FSDataInputStream fs, int singleCdrSize) {

		try {

			// rowkey���ļ�ID + �ļ�ƫ��
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
			
			try
			{
				byte[] content = new byte[size];
				fs.read(content,0,size);
				System.arraycopy(content, 0, start_time_s, 0, 4);
				System.arraycopy(content, 4, start_time_ns, 0, 4);
				System.arraycopy(content, 8, end_time_s, 0, 4);
				System.arraycopy(content, 12, end_time_ns , 0, 4);
				System.arraycopy(content, 16, cdr_index, 0, 4);
				System.arraycopy(content, 20, cdr_type, 0, 1);
				System.arraycopy(content, 21, cdr_result, 0, 1);
				System.arraycopy(content, 22, base_cdr_index, 0, 4);
				System.arraycopy(content, 26, base_cdr_type, 0, 1);
				
				System.arraycopy(content, 27, tmsi, 0, 4);
				System.arraycopy(content, 31, new_tmsi, 0, 4);
				System.arraycopy(content, 35, imsi, 0, 8);
				System.arraycopy(content, 43, imei, 0, 8);
				System.arraycopy(content, 51, calling_number, 0, 8);	
				System.arraycopy(content, 59, called_number, 0, 8);
				
				System.arraycopy(content, 67, third_number, 0, 24);
				System.arraycopy(content, 91, mgw_ip, 0, 4);
				System.arraycopy(content, 95, msc_server_ip, 0, 4);
				System.arraycopy(content, 99, bsc_spc, 0, 4);
				System.arraycopy(content, 103, msc_spc, 0, 4);
				System.arraycopy(content, 107, lac, 0, 2);
				
				System.arraycopy(content, 109, ci, 0, 2);
				System.arraycopy(content, 111, last_lac, 0, 2);
				System.arraycopy(content, 113, last_ci, 0, 2);
				System.arraycopy(content, 115, cref_cause, 0, 1);
				System.arraycopy(content, 116, cm_rej_cause, 0, 1);
				System.arraycopy(content, 117, lu_rej_cause, 0, 1);
				
				System.arraycopy(content, 118, assign_failure_cause , 0, 1);
				System.arraycopy(content, 119, rr_cause, 0, 1);
				System.arraycopy(content, 120, cip_rej_cause, 0, 1);
				System.arraycopy(content, 121, disconnect_cause, 0, 1);
				System.arraycopy(content, 122, cc_rel_cause, 0, 1);
				System.arraycopy(content, 123, clear_cause , 0, 1);
				System.arraycopy(content, 124, cp_cause, 0, 1);
				
				System.arraycopy(content, 125, rp_cause, 0, 1);
				System.arraycopy(content, 126, ho_cause, 0, 24);
				System.arraycopy(content, 150, ho_failure_cause, 0, 24);
				System.arraycopy(content, 174, first_paging_time, 0, 4);
				System.arraycopy(content, 178, second_paging_time, 0, 4);
				System.arraycopy(content, 182, third_paging_time, 0, 4);
				System.arraycopy(content, 186, fourth_paging_time, 0, 4);
				System.arraycopy(content, 190, cc_time, 0, 4);
				System.arraycopy(content, 194, assignment_time, 0, 4);
				
				System.arraycopy(content, 198, assignment_cmp_time, 0, 4);
				System.arraycopy(content, 202, setup_time, 0, 4);
				System.arraycopy(content, 206, alert_time, 0, 4);
				System.arraycopy(content, 210, connect_time, 0, 4);
				System.arraycopy(content, 214, disconnect_time, 0, 4);
				System.arraycopy(content, 218, clear_request_time, 0, 4);
				System.arraycopy(content, 222, clear_command_time, 0, 4);
				System.arraycopy(content, 226, rp_data_time, 0, 4);
				
				System.arraycopy(content, 230, rp_ack_time, 0, 4);
				System.arraycopy(content, 234, aut_request_time, 0, 4);
				System.arraycopy(content, 238, aut_response_time, 0, 4);
				System.arraycopy(content, 242, cm_service_accept_time , 0, 4);
				System.arraycopy(content, 246, call_confirm_preceding_time, 0, 4);
				System.arraycopy(content, 250, connect_ack_time, 0, 4);
				System.arraycopy(content, 254, smsc, 0, 24);
				
				System.arraycopy(content, 278, sm_type, 0, 1);
				System.arraycopy(content, 279, sm_data_coding_scheme, 0, 1);
				System.arraycopy(content, 280, sm_length, 0, 2);
				System.arraycopy(content, 282, rp_data_count, 0, 1);
				System.arraycopy(content, 283, handover_count, 0, 1);
				
				System.arraycopy(content, 284, info_trans_capability, 0, 1);
				System.arraycopy(content, 285, speech_version, 0, 1);
				System.arraycopy(content, 286, failed_handover_count, 0, 1);
				System.arraycopy(content, 287, sub_cdr_index_set, 0, 200);
				
				System.arraycopy(content, 487, sub_cdr_starttime_set, 0, 200);
				System.arraycopy(content, 687, call_stop, 0, 1);
				System.arraycopy(content, 688, interbsc_ho_count, 0, 1);
				System.arraycopy(content, 689, pcmts, 0, 2);
				
				System.arraycopy(content, 691, bscid, 0, 8);
				System.arraycopy(content, 699, call_stop_msg, 0, 1);
				System.arraycopy(content, 700, call_stop_cause, 0, 1);
				System.arraycopy(content, 701, mscid, 0, 4);
				System.arraycopy(content, 705, last_bscid, 0, 8);
				
				System.arraycopy(content, 713, last_mscid, 0, 4);
				System.arraycopy(content, 717, dtmf, 0, 1);
				System.arraycopy(content, 718, cid, 0, 4);
				System.arraycopy(content, 722, last_cid, 0, 4);
				System.arraycopy(content, 726, tac, 0, 4);
				System.arraycopy(content, 730, cdr_rel_type, 0, 1);
				
				
				System.arraycopy(content, 731, last_pcm, 0, 2);
				System.arraycopy(content, 733, identity_request_time , 0, 4);
				System.arraycopy(content, 737, identity_response_time, 0, 4);
				System.arraycopy(content, 741, ciph_mode_cmd_time, 0, 4);
				System.arraycopy(content, 745, ciph_mode_cmp_time, 0, 4);
				System.arraycopy(content, 749, tmsi_realloc_cmd_time, 0, 4);
				System.arraycopy(content, 753, tmsi_realloc_cmp_time, 0, 4);
				System.arraycopy(content, 757, cc_release_time , 0, 4);
				System.arraycopy(content, 761, cc_release_cmp_time, 0, 4);
				System.arraycopy(content, 765, clear_cmp_time, 0, 4);
				System.arraycopy(content, 769, sccp_release_time, 0, 4);
				System.arraycopy(content, 773, sccp_release_cmp_time, 0, 4);
			}catch(Exception e)
			{
				e.printStackTrace();
			}
			
			/*
			 * index tables needed are call num index; time index; and cell
			 * index; and all kinds of KPI related index;
			 */
			ConstructCallingNumIndexItem(rowKey);
			MSCConstructCallingNumIndexItem(rowKey);
			BSCConstructCallingNumIndexItem(rowKey);
			CIDConstructCallingNumIndexItem(rowKey);
			
			ConstructCalledNumIndexItem(rowKey);
			MSCConstructCalledNumIndexItem(rowKey);
			BSCConstructCalledNumIndexItem(rowKey);
			CIDConstructCalledNumIndexItem(rowKey);
			

		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	private void  MSCConstructCalledNumIndexItem(byte[] rowKey) {
		int i = 0;
		for (i = 0; i < 4; i++){
			if (mscid[i] != 0){
				break;
			}
		}
		if (4 == i){
			return;
		}
		
		for (i = 0; i < 8; i++){
			if (called_number[i] != 0){
				break;
			}
		}
		if (8 == i){
			return;
		}
		byte[] mscCalledNumTime = new byte[21];		
		System.arraycopy(mscid, 0, mscCalledNumTime, 0, 4);
		System.arraycopy(called_number, 0, mscCalledNumTime, 4, 8);
		System.arraycopy(start_time_s, 0, mscCalledNumTime, 12, 4);
		System.arraycopy(cdr_type, 0, mscCalledNumTime, 16, 1);
		System.arraycopy(cdr_index, 0, mscCalledNumTime, 17, 4);

		Put p = new Put(mscCalledNumTime);
		p.add(Bytes.toBytes("Offset"), null,rowKey);
		mscputCalledIndexItemList.add(p);
	}
	
	private void  BSCConstructCalledNumIndexItem(byte[] rowKey) {
		int i = 0;
		for (i = 0; i < 8; i++){
			if (bscid[i] != 0){
				break;
			}
		}
		if (8 == i){
			return;
		}
		
		for (i = 0; i < 8; i++){
			if (called_number[i] != 0){
				break;
			}
		}
		if (8 == i){
			return;
		}

		byte[] bscCalledNumTime = new byte[25];		
		System.arraycopy(bscid, 0, bscCalledNumTime, 0, 8);
		System.arraycopy(called_number, 0, bscCalledNumTime, 8, 8);
		System.arraycopy(start_time_s, 0, bscCalledNumTime, 16, 4);
		System.arraycopy(cdr_type, 0, bscCalledNumTime, 20, 1);
		System.arraycopy(cdr_index, 0, bscCalledNumTime, 21, 4);
		
		Put p = new Put(bscCalledNumTime);
		p.add(Bytes.toBytes("Offset"), null,rowKey);
		bscputCalledIndexItemList.add(p);
	}
	
	private void  CIDConstructCalledNumIndexItem(byte[] rowKey) {
		int i = 0;
		for (i = 0; i < 4; i++){
			if (cid[i] != 0){
				break;
			}
		}
		if (4 == i){
			return;
		}
		
		for (i = 0; i < 8; i++){
			if (called_number[i] != 0){
				break;
			}
		}
		if (8 == i){
			return;
		}
		byte[] cidCalledNumTime = new byte[21];		
		System.arraycopy(cid, 0, cidCalledNumTime, 0, 4);
		System.arraycopy(called_number, 0, cidCalledNumTime, 4, 8);
		System.arraycopy(start_time_s, 0, cidCalledNumTime, 12, 4);
		System.arraycopy(cdr_type, 0, cidCalledNumTime, 16, 1);
		System.arraycopy(cdr_index, 0, cidCalledNumTime, 17, 4);

		Put p = new Put(cidCalledNumTime);
		p.add(Bytes.toBytes("Offset"), null,rowKey);
		cidputCalledIndexItemList.add(p);
	}
	
	private void ConstructCalledNumIndexItem(byte[] rowKey) {
		int i = 0;
		for (i = 0; i < 8; i++){
			if (called_number[i] != 0){
				break;
			}
		}
		if (8 == i){
			return;
		}
		byte[] calledNumTime = new byte[17];		
		System.arraycopy(called_number, 0, calledNumTime, 0, 8);
		System.arraycopy(start_time_s, 0, calledNumTime, 8, 4);
		System.arraycopy(cdr_type, 0, calledNumTime, 12, 1);
		System.arraycopy(cdr_index, 0, calledNumTime, 13, 4);
		
		Put p = new Put(calledNumTime);
		p.add(Bytes.toBytes("Offset"), null,rowKey);
		putCalledIndexItemList.add(p);
	}

	private void ConstructCallingNumIndexItem(byte[] rowKey) {
		// calling num + start_time_s
		int i = 0;
		for (i = 0; i < 8; i++){
			if (calling_number[i] != 0){
				break;
			}
		}
		if (8 == i){
			return;
		}
		byte[] callingNumTime = new byte[17];		
		System.arraycopy(calling_number, 0, callingNumTime, 0, 8);
		System.arraycopy(start_time_s, 0, callingNumTime, 8, 4);
		System.arraycopy(cdr_type, 0, callingNumTime, 12, 1);
		System.arraycopy(cdr_index, 0, callingNumTime, 13, 4);

		Put p = new Put(callingNumTime);
		p.add(Bytes.toBytes("Offset"), null, rowKey);
		putCallingIndexItemList.add(p);
	}
	
	private void MSCConstructCallingNumIndexItem(byte[] rowKey) {
		// calling num + start_time_s
		int i = 0;
		for (i = 0; i < 4; i++){
			if (mscid[i] != 0){
				break;
			}
		}
		if (4 == i){
			return;
		}
		
		for (i = 0; i < 8; i++){
			if (calling_number[i] != 0){
				break;
			}
		}
		if (8 == i){
			return;
		}
		byte[]  mscCallingNumTime = new byte[21];		
						
		System.arraycopy(mscid, 0, mscCallingNumTime, 0, 4);
		System.arraycopy(calling_number, 0, mscCallingNumTime, 4, 8);
		System.arraycopy(start_time_s, 0, mscCallingNumTime, 12, 4);
		System.arraycopy(cdr_type, 0, mscCallingNumTime, 16, 1);
		System.arraycopy(cdr_index, 0, mscCallingNumTime, 17, 4);

		Put p = new Put(mscCallingNumTime);
		p.add(Bytes.toBytes("Offset"), null,rowKey);
		mscputCalledIndexItemList.add(p);
	}
	
	private void BSCConstructCallingNumIndexItem(byte[] rowKey) {
		// calling num + start_time_s
		int i = 0;
		for (i = 0; i < 8; i++){
			if (bscid[i] != 0){
				break;
			}
		}
		if (8 == i){
			return;
		}
		for (i = 0; i < 8; i++){
			if (calling_number[i] != 0){
				break;
			}
		}
		if (8 == i){
			return;
		}
		byte[]  bscCallingNumTime = new byte[25];		
		System.arraycopy(bscid, 0, bscCallingNumTime, 0, 8);
		System.arraycopy(calling_number, 0, bscCallingNumTime, 8, 8);
		System.arraycopy(start_time_s, 0, bscCallingNumTime, 16, 4);
		System.arraycopy(cdr_type, 0, bscCallingNumTime, 20, 1);
		System.arraycopy(cdr_index, 0, bscCallingNumTime, 21, 4);
		Put p = new Put(bscCallingNumTime);
		p.add(Bytes.toBytes("Offset"), null, rowKey);
		bscputCallingIndexItemList.add(p);
	}
	
	private void  CIDConstructCallingNumIndexItem(byte[] rowKey) {
		// calling num + start_time_s
		int i = 0;
		for (i = 0; i < 4; i++){
			if (cid[i] != 0){
				break;
			}
		}
		if (4 == i){
			return;
		}
		
		for (i = 0; i < 8; i++){
			if (calling_number[i] != 0){
				break;
			}
		}
		if (8 == i){
			return;
		}

		byte[]  cidCallingNumTime = new byte[21];		
		System.arraycopy(cid, 0, cidCallingNumTime, 0, 4);
		System.arraycopy(calling_number, 0, cidCallingNumTime, 4, 8);
		System.arraycopy(start_time_s, 0, cidCallingNumTime, 12, 4);
		System.arraycopy(cdr_type, 0, cidCallingNumTime, 16, 1);
		System.arraycopy(cdr_index, 0, cidCallingNumTime, 17, 4);

		Put p = new Put(cidCallingNumTime);
		p.add(Bytes.toBytes("Offset"), null, rowKey);
		cidputCallingIndexItemList.add(p);
	}


	public void Clear() {
//		this.putDataList.clear();
		this.putCalledIndexItemList.clear();
		this.putCallingIndexItemList.clear();
		this.mscputCalledIndexItemList.clear();
		this.mscputCallingIndexItemList.clear();
		this.bscputCalledIndexItemList.clear();
		this.bscputCallingIndexItemList.clear();
		this.cidputCalledIndexItemList.clear();
		this.cidputCallingIndexItemList.clear();
		
	}

	public void Commit() {
		try {
			// put called and calling index items
			if (this.putCalledIndexItemList.isEmpty() == false) {
				System.out
						.println("insert to hbase_bassap_cdr_Called_Index, item count: "
								+ this.putCalledIndexItemList.size());
				this.hBSSAPtableCalledIndex.put(this.putCalledIndexItemList);
				this.putCalledIndexItemList.clear();
				this.hBSSAPtableCalledIndex.flushCommits();
			}

			if (this.putCallingIndexItemList.isEmpty() == false) {
				System.out
						.println("insert to hbase_bassap_cdr_CallingIndex, item count: "
								+ this.putCallingIndexItemList.size());
				this.hBSSAPtableCallingIndex.put(this.putCallingIndexItemList);
				this.putCallingIndexItemList.clear();
				this.hBSSAPtableCallingIndex.flushCommits();
			}
			
			//msc
			if (this.mscputCalledIndexItemList.isEmpty() == false) {
				System.out
						.println("insert to hbase_bassap_cdr_msc_Called_Index, item count: "
								+ this.mscputCalledIndexItemList.size());
				this.hMSCBSSAPtableCalledIndex.put(this.mscputCalledIndexItemList);
				this.mscputCalledIndexItemList.clear();
				this.hMSCBSSAPtableCalledIndex.flushCommits();
			}

			if (this.mscputCallingIndexItemList.isEmpty() == false) {
				System.out
						.println("insert to hbase_bassap_cdr_msc_CallingIndex, item count: "
								+ this.mscputCallingIndexItemList.size());
				this.hMSCBSSAPtableCallingIndex.put(this.mscputCallingIndexItemList);
				this.mscputCallingIndexItemList.clear();
				this.hMSCBSSAPtableCallingIndex.flushCommits();
			}
			
			//bsc
			if (this.bscputCalledIndexItemList.isEmpty() == false) {
				System.out
						.println("insert to hbase_bassap_cdr_bsc_Called_Index, item count: "
								+ this.bscputCalledIndexItemList.size());
				this.hBSCBSSAPtableCalledIndex.put(this.bscputCalledIndexItemList);
				this.bscputCalledIndexItemList.clear();
				this.hBSCBSSAPtableCalledIndex.flushCommits();
			}

			if (this.bscputCallingIndexItemList.isEmpty() == false) {
				System.out
						.println("insert to hbase_bassap_cdr_bsc_CallingIndex, item count: "
								+ this.bscputCallingIndexItemList.size());
				this.hBSCBSSAPtableCallingIndex.put(this.bscputCallingIndexItemList);
				this.bscputCallingIndexItemList.clear();
				this.hBSCBSSAPtableCallingIndex.flushCommits();
			}
			
			
			//cid
			if (this.cidputCalledIndexItemList.isEmpty() == false) {
				System.out
						.println("insert to hbase_bassap_cdr_cid_Called_Index, item count: "
								+ this.cidputCalledIndexItemList.size());
				this.hCIDBSSAPtableCalledIndex.put(this.cidputCalledIndexItemList);
				this.cidputCalledIndexItemList.clear();
				this.hCIDBSSAPtableCalledIndex.flushCommits();
			}

			if (this.cidputCallingIndexItemList.isEmpty() == false) {
				System.out
						.println("insert to hbase_bassap_cdr_cid_CallingIndex, item count: "
								+ this.cidputCallingIndexItemList.size());
				this.hCIDBSSAPtableCallingIndex.put(this.cidputCallingIndexItemList);
				this.cidputCallingIndexItemList.clear();
				this.hCIDBSSAPtableCallingIndex.flushCommits();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
