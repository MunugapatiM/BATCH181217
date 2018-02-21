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
import com.laboros.mr.partitioner.MyPartitioner;
import com.laboros.reducer.WordCountReducer;


//yarn jar client.jar com.laboros.mr.WordCountDriver input output
public class CPDriver extends Configured implements Tool {
	public static void main(String[] args) {

		//Total = 3 Steps
		//1) Validation
		if(args.length<2)
		{
			System.out.println("Usage "+CPDriver.class.getName()+" " +
					"/path/to/hdfs/input /path/to/hdfs/output");
			return;
		}
		//2) Loading configuration
		Configuration conf = new Configuration(Boolean.TRUE);
		//3) ToolRunner.run
		try {
			int i = ToolRunner.run(conf, new CPDriver(), args);
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
		Job cpJob = Job.getInstance(conf, CPDriver.class.getName());
		//step:3 : Setting the classpath to mapper on Datanode
		cpJob.setJarByClass(CPDriver.class);
		
		//step:4 Setting the input
		final String input = args[0];
		final Path inputPath=new Path(input);
		TextInputFormat.addInputPath(cpJob, inputPath);
		cpJob.setInputFormatClass(TextInputFormat.class);
		
		//step:5 Setting the output
		
		final String output=args[1];
		final Path outputPath = new Path(output);
		TextOutputFormat.setOutputPath(cpJob, outputPath);
		cpJob.setOutputFormatClass(TextOutputFormat.class);
		//step:6 Setting the mapper
		
		cpJob.setMapperClass(WordCountMapper.class);
		
		//step:7 Setting the mapper output key and value class
		cpJob.setMapOutputKeyClass(Text.class);
		cpJob.setMapOutputValueClass(IntWritable.class);
		
		//step:8: Setting the reducer
		cpJob.setReducerClass(WordCountReducer.class);
		//setting the combiner and Partitioner
		cpJob.setCombinerClass(WordCountReducer.class);
		cpJob.setPartitionerClass(MyPartitioner.class);
		cpJob.setNumReduceTasks(3);
		
		
		
		//step:9: Setting the reducer output key and value class
		cpJob.setOutputKeyClass(Text.class);
		cpJob.setOutputValueClass(IntWritable.class);
		//step:10  : Trigger Method
		cpJob.waitForCompletion(Boolean.TRUE);
		return 0;
	}
}
