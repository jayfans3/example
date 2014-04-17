package com.ailk.oci.ocnosql.client.importdata.phoenix;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
public class TestPhoenixCSVLoader {
	/**
	 * 结论:<br>
	 * csv文件必须跟表名一致，sql文件不需要跟表名一致
	 * sql文件必须在csv文件的前面
	 * 多表导入有问题
	 */
	@Test
	public void testLoadFromFile(){
		try {
			PhoenixCSVLoadUtil.load("ocdata05,ocdata06,ocdata07:2485", "D:\\test\\test01.sql", "D:\\test\\test01.csv");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	@Test
	public void testBatchLoadFromFile(){
		Map<String,String>  pathMap=new HashMap<String,String>();
		pathMap.put("D:\\lifei1.sql", "D:\\lifei.csv");
		pathMap.put("D:\\liujunshan1.sql", "D:\\liujunshan.csv");
		try {
			PhoenixCSVLoadUtil.batchLoadFromFile("ocdata05,ocdata06,ocdata07:2485", pathMap);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

    /**
     * 测试从多个目录导入csv数据到指定的某个表
     */
    @Test
	public void testLoadFromDir(){
		try {
            //-z ocdata05,ocdata06,ocdata07:2485 -t test01 -h id,name,tel -s D:\\test01.sql -c D:\\test1,D:\\test2 -i 0
            String[] args=new String[]{"-t","test01","-h","id,f.user_id,f.name,f.tel","-s","D:\\test01.sql","-c","D:\\test1,D:\\test2"};
            PhoenixCSVLoadUtil.main(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
