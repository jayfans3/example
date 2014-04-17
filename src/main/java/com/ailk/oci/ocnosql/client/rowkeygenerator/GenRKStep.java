package com.ailk.oci.ocnosql.client.rowkeygenerator;

/**
 * Created by IntelliJ IDEA.
 * User: lile3
 * Date: 13-11-5
 * Time: 下午1:54
 * To change this template use File | Settings | File Templates.
 */
//生成rowkey的步骤
public class GenRKStep {

    /*
       指定哪几个字段生成rowkey,多个字段以逗号分隔，且有序。
       如果没有指定，则是对前面所有步骤后的结果字符串做处理。
       如 2,1,3
    */
    private String rkIndexes;

    /* 对指定的选择字段所采用的算法，如MD5，HASH...如果没有则不填。 */
    private String algo;

    /* 对算法之后的字符串执行的处理规则类 如com.ailk.ocnosql.CallBack1 */
    private String callBack;

    public GenRKStep() {
    }

    public GenRKStep(String rkIndexes, String algo, String callBack) {
        this.rkIndexes = rkIndexes;
        this.algo = algo;
        this.callBack = callBack;
    }

    public String getRkIndexes() {
        return rkIndexes;
    }

    public void setRkIndexes(String rkIndexes) {
        this.rkIndexes = rkIndexes;
    }

    public String getAlgo() {
        return algo;
    }

    public void setAlgo(String algo) {
        this.algo = algo;
    }

    public String getCallBack() {
        return callBack;
    }

    public void setCallBack(String callBack) {
        this.callBack = callBack;
    }
}
