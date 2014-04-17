package com.ailk.oci.ocnosql.client.rowkeygenerator;

/**
 * Created by IntelliJ IDEA.
 * User: lile3
 * Date: 13-11-5
 * Time: 下午2:06
 * To change this template use File | Settings | File Templates.
 */
public interface GenRKCallBack {

    /*调用完算法之后，回调处理函数*/
    String callback(String rowKey,String line);
}
