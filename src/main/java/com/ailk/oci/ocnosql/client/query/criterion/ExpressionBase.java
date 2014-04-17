package com.ailk.oci.ocnosql.client.query.criterion;

/**
 * Created by IntelliJ IDEA.
 * User: wucs
 * Date: 13-11-11
 * Time: 上午10:33
 * To change this template use File | Settings | File Templates.
 */
public abstract class ExpressionBase implements Expression{
    protected String colFamily;
    protected String col;
    protected  ExpressionBase(String colFamily,String col){
        this.colFamily = colFamily;
        this.col = col;
    }
    public String getColFamily() {
        return colFamily;
    }

    public String getCol() {
        return col;
    }

    public void setColFamily(String colFamily) {
        this.colFamily = colFamily;
    }

    public void setCol(String col) {
        this.col = col;
    }
}
