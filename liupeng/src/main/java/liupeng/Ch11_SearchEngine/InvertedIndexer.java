import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.wltea.analyzer.IKSegmentation;
import org.wltea.analyzer.Lexeme;
import nju.lily.*;
import java.util.Map; 


public class InvertedIndexer 
{
	public static class InvertedIndexerMapper  extends Mapper<LongWritable, Text, Text, SingleRecordWritable>
	{
		public final static Text word = new Text();
		public final static SingleRecordWritable singleRecord = new SingleRecordWritable();
		
		public void map(LongWritable key,Text val,Context context
		) throws IOException, InterruptedException 
		{
			String[] str=val.toString().split("\007", 5);
			HashMap<String, Rank_Positions> TokenMap=new HashMap<String, Rank_Positions>();
			
			StringReader strreader_title=new StringReader(str[3]);
			IKSegmentation IK_title=new IKSegmentation(strreader_title);
			Lexeme lex=new Lexeme(0, 0, 0, 0);
			while((lex=IK_title.next())!=null)
			{
				if(lex.getLength()>=2)
				{
					String token = lex.getLexemeText();
					int start=lex.getBeginPosition();
					int end=lex.getEndPosition();
					if(!TokenMap.containsKey(token))
						TokenMap.put(token,new Rank_Positions(10,true,start,end));
					else
						TokenMap.put(token, TokenMap.get(token).Add(10,true,start,end));	
				}
				/*
				if(TokenMap.size()>100)
				{
					this.EmitMapValue(TokenMap,key,context);
					TokenMap.clear();
				}
				*/
					
			}
			
			StringReader strreader_content=new StringReader(str[4]);
			IKSegmentation IK_content=new IKSegmentation(strreader_content);
			Lexeme lex_content=new Lexeme(0, 0, 0, 0);
			while((lex_content=IK_content.next())!=null)
			{
				if(lex_content.getLength()>=2)
				{
					String token_content = lex_content.getLexemeText();
					int start=lex_content.getBeginPosition();
					int end=lex_content.getEndPosition();
					if(!TokenMap.containsKey(token_content))
						TokenMap.put(token_content,new Rank_Positions((float)(100.0/Float.valueOf(str[4].length())),false,start,end));
					else
						TokenMap.put(token_content,TokenMap.get(token_content).Add((float)(500.0/Float.valueOf(str[4].length())),false,start,end));
				}
				/*
				if(TokenMap.size()>100)
				{
					this.EmitMapValue(TokenMap,key,context);
					TokenMap.clear();
				}
				*/
			}
			this.EmitMapValue(TokenMap,key,context);
			TokenMap.clear();
		}
		
		public void EmitMapValue(HashMap<String, Rank_Positions> TokenMap,LongWritable key,Context context) throws IOException, InterruptedException
		{
			for(Map.Entry<String,Rank_Positions> entry:TokenMap.entrySet())
			{
				word.set(entry.getKey());
				singleRecord.set(key.get(), entry.getValue());
				context.write(word, singleRecord);
			}
		}
	}
	
	//public static class InvertedIndexerCombiner extends 
	public static class InvertedIndexerReducer extends Reducer<Text,SingleRecordWritable,Text,Text> 
	{
		public void reduce(Text key, Iterable<SingleRecordWritable> values,Context context) throws IOException, InterruptedException 
		{
			boolean first = true;
			String toReturn="";
			for (SingleRecordWritable record : values)
			{
				if(!first)
					toReturn+=";";
				first=false;				
				toReturn+=record.GetDID().toString()+":"+record.GetRank().toString()+":"+record.GetPositions().toString();
			}
			Text out=new Text(toReturn);
			context.write(key,out);
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException 
	{
		Configuration conf = new Configuration();
		String path0="/test.txt";
		String path1="/outputdata/testdata/out11";
		Job job = new Job(conf, "Line Indexer");
		job.setInputFormatClass(TextInputFormat.class);
	    job.setJarByClass(InvertedIndexer.class);
	    job.setMapperClass(InvertedIndexerMapper.class);
	    //job.setCombinerClass(InvertedIndexerReducer.class);
	    job.setReducerClass(InvertedIndexerReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(SingleRecordWritable.class);
	    FileInputFormat.addInputPath(job, new Path(path0));
	    FileOutputFormat.setOutputPath(job, new Path(path1));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
