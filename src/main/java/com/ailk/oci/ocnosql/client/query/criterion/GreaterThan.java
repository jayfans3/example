/* ================================================================
 * JSQLParser : java based sql parser 
 * ================================================================
 *
 * Project Info:  http://jsqlparser.sourceforge.net
 * Project Lead:  Leonardo Francalanci (leoonardoo@yahoo.it);
 *
 * (C) Copyright 2004, by Leonardo Francalanci
 *
 * This library is free software; you can redistribute it and/or modify it under the terms
 * of the GNU Lesser General Public License as published by the Free Software Foundation;
 * either version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330,
 * Boston, MA 02111-1307, USA.
 */

package com.ailk.oci.ocnosql.client.query.criterion;


import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("unchecked")
public class GreaterThan extends ExpressionBase {


    Comparable value;

    public GreaterThan(String colFamily, String col,Comparable value) {
        super(colFamily,col);
        this.value = value;
    }

    public boolean accept(Comparable detail) {
        Comparable v = null;

        if (value instanceof String) {
            return detail.compareTo(value) > 0 ? true : false;
        }
        if (value instanceof Integer) {
            v = Integer.decode(detail.toString());
        }
        if (value instanceof Double) {
            v = Double.parseDouble(detail.toString());
        }
        if (value instanceof Long) {
            v = Long.parseLong(detail.toString());
        }
        if (value instanceof Float) {
            v = Float.parseFloat(detail.toString());
        }
        return v.compareTo(value) > 0 ? true : false;
    }

    public Filter trans2filter(){
        return new SingleColumnValueFilter(Bytes.toBytes(colFamily),Bytes.toBytes(col), CompareFilter.CompareOp.GREATER, Bytes.toBytes(String.valueOf(value)));
    }
}
