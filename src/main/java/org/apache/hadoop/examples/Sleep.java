/**
 * 
 */
package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * @author liujs3
 *
 */
public class Sleep extends Configured implements Tool{

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}

	@Override
	public int run(String[] arg0) throws Exception {
		if (arg0.length != 2) {
		      System.err.println("Usage: sleelp <in> <out>");
		      System.exit(2);
		      return -1;
		    }
		
		Job job=new Job(getConf(),"sleep");
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
	    FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
	    
		
		
	}
	

}
