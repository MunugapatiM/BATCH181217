package com.laboros.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.WordCountMapper;
import com.laboros.reducer.WordCountReducer;


//yarn jar client.jar com.laboros.mr.WordCountDriver input output
public class WordCountDriver extends Configured implements Tool {
	public static void main(String[] args) {

		//Total = 3 Steps
		//1) Validation
		if(args.length<2)
		{
			System.out.println("Usage "+WordCountDriver.class.getName()+" " +
					"/path/to/hdfs/input /path/to/hdfs/output");
			return;
		}
		//2) Loading configuration
		Configuration conf = new Configuration(Boolean.TRUE);
		//3) ToolRunner.run
		try {
			int i = ToolRunner.run(conf, new WordCountDriver(), args);
			if(i==0){
				System.out.println("SUCCESS");
			}else{
				System.out.println("FAILURE");
			}
		} catch (Exception e) {
			System.out.println("FAILURE");
			e.printStackTrace();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {

		//TOTAL 10 steps
		//Step: 1 Get the configuration
		Configuration conf = super.getConf();
		
		//NOTE: Set anything to configuration only here
		
		//Step 2: Create Job Instance
		Job wordCountJob = Job.getInstance(conf, WordCountDriver.class.getName());
		//step:3 : Setting the classpath to mapper on Datanode
		wordCountJob.setJarByClass(WordCountDriver.class);
		
		//step:4 Setting the input
		final String input = args[0];
		final Path inputPath=new Path(input);
		TextInputFormat.addInputPath(wordCountJob, inputPath);
		wordCountJob.setInputFormatClass(TextInputFormat.class);
		
		//step:5 Setting the output
		
		final String output=args[1];
		final Path outputPath = new Path(output);
		TextOutputFormat.setOutputPath(wordCountJob, outputPath);
		wordCountJob.setOutputFormatClass(TextOutputFormat.class);
		//step:6 Setting the mapper
		
		wordCountJob.setMapperClass(WordCountMapper.class);
		
		//step:7 Setting the mapper output key and value class
		wordCountJob.setMapOutputKeyClass(Text.class);
		wordCountJob.setMapOutputValueClass(IntWritable.class);
		
		//step:8: Setting the reducer
		wordCountJob.setReducerClass(WordCountReducer.class);
		//step:9: Setting the reducer output key and value class
		wordCountJob.setOutputKeyClass(Text.class);
		wordCountJob.setOutputValueClass(IntWritable.class);
		//step:10  : Trigger Method
		wordCountJob.waitForCompletion(Boolean.TRUE);
		return 0;
	}
}
