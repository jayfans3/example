package nju.lily;

import java.io.*;
import org.apache.hadoop.io.*;
import nju.lily.Rank_Positions;

public class SingleRecordWritable implements WritableComparable<SingleRecordWritable> {
	
	private LongWritable DID;
	private FloatWritable rank;
	private Text positions;
	
	public SingleRecordWritable(){
		set(new LongWritable(),new FloatWritable(),new Text());
	}
	
	public LongWritable GetDID(){
		return this.DID;
	}
	
	public FloatWritable GetRank(){
		return this.rank;
	}
	
	public Text GetPositions()
	{
		return new Text(this.positions.toString());
	}
	
	public void set(LongWritable t_DID,FloatWritable t_rank,Text t_positions){
		this.DID=t_DID;
		this.rank=t_rank;
		this.positions=t_positions;
	}
	
	public void set(long t_DID,Rank_Positions t_rankpositions){
		set(new LongWritable(t_DID),new FloatWritable(t_rankpositions.rank),new Text(t_rankpositions.GetPosiitons()));
	}
	
	public void setDID(long t_DID)
	{
		this.DID=new LongWritable(t_DID);
	}
	
	public void setRank(float rank)
	{
		this.rank=new FloatWritable(rank);
	}
	
	public void setPositions(Text t_positions)
	{
		this.positions=new Text(t_positions);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		DID.readFields(in);
		rank.readFields(in);
		this.positions.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.DID.write(out);
		this.rank.write(out);
		this.positions.write(out);
	}
	
	@Override
	public boolean equals(Object o){
		if(o instanceof SingleRecordWritable){
			SingleRecordWritable tmp=(SingleRecordWritable)o;
			return this.DID.equals(tmp.DID)&&this.rank.equals(tmp.rank)&&this.positions.equals(tmp.positions);
		}
		return false;
	}
	@Override
	public String toString()
	{
	return this.DID.toString()+"\t"+this.rank.toString()+"\t"+this.positions.toString();	
	
	}
	@Override
	public int compareTo(SingleRecordWritable tmp) {
		// TODO Auto-generated method stub
		return this.DID.compareTo(tmp.DID);
	}
}
