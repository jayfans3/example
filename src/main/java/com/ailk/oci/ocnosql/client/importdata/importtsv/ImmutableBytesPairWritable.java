package com.ailk.oci.ocnosql.client.importdata.importtsv;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-5-21
 * Time: 下午3:01
 * To change this template use File | Settings | File Templates.
 */
public class ImmutableBytesPairWritable implements WritableComparable<ImmutableBytesPairWritable> {
    private byte[] first;
    private byte[] second;

    public ImmutableBytesPairWritable() {
    }

    public ImmutableBytesPairWritable(byte[] first, byte[] second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(ImmutableBytesPairWritable o) {
        int result = Bytes.compareTo(this.first, o.first);
        if (result != 0) {
            return result;
        }
        return Bytes.compareTo(this.second, o.second);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Bytes.writeByteArray(dataOutput, first);
        Bytes.writeByteArray(dataOutput, second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first = Bytes.readByteArray(dataInput);
        this.second = Bytes.readByteArray(dataInput);
    }

    public byte[] getFirst() {
        return first;
    }

    public void setFirst(byte[] first) {
        this.first = first;
    }

    public byte[] getSecond() {
        return second;
    }

    public void setSecond(byte[] second) {
        this.second = second;
    }
}
