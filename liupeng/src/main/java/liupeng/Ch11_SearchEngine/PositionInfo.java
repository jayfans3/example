package nju.lily;
public class PositionInfo {
 
 StringBuilder positions;
 public PositionInfo(){
	 this.positions =new StringBuilder(); 
 }
 
 public PositionInfo(boolean Is_Title,int t_start,int t_end)
{
	 this.positions=new StringBuilder();
	 this.Add(Is_Title, t_start, t_end);
}
 public String GetPositions()
 {
	 if(this.positions!=null)
		 return this.positions.toString().substring(0, this.positions.toString().length()-1);
	 else
		 return "";
 }
 public void Add(boolean IsTitle,int start,int end)
 {
	 String tmp="";
	 if(IsTitle)
		 tmp+="1|";
	 else
		 tmp+="0|";
	 tmp+=String.valueOf(start)+"|";
	 tmp+=String.valueOf(end)+"%";
	 
	 this.positions.append(tmp);
 }
}
