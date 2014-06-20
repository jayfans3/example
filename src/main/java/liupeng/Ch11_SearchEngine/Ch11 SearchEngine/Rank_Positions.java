package nju.lily;

import nju.lily.PositionInfo;
public class Rank_Positions {

	float rank;
	PositionInfo positions;
	public Rank_Positions()
	{
		this.rank=0;
		this.positions =new PositionInfo();
	}
	
	public Rank_Positions(float _t_rankrank,boolean t_IsTitle,int t_start,int t_end)
	{
		this.rank=_t_rankrank;
		this.positions=new PositionInfo(t_IsTitle,t_start,t_end);
	}
	
	public Rank_Positions Add(float t_rank,boolean IsTitle,int start,int end)
	{
		this.rank+=t_rank;
		this.positions.Add(IsTitle, start, end);
		return this;
	}
	
	public float GetRank()
	{
		return this.rank;
	}
	
	public String GetPosiitons()
	{
		return this.positions.GetPositions();
	}
}

