package com.ailk.oci.ocnosql.client.rowkeygenerator;

import org.junit.Test;

/**
 * Created by IntelliJ IDEA.
 * User: lifei5
 * Date: 13-12-12
 * Time: 上午11:37
 * To change this template use File | Settings | File Templates.
 */
public class TestGetRowKeyUtil {
    @Test
	public void testLoadFromDir(){
		try {
            String rowKey=GetRowKeyUtil.getRowKeyByStringArr("TEST01",new String[]{"1","jing","15652965156"});

            System.out.println("----------------rowKey="+rowKey);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
