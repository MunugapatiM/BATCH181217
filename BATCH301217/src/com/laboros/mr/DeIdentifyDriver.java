package com.laboros.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


//yarn jar client.jar com.laboros.mr.DeIdentify input output
public class DeIdentifyDriver extends Configured implements Tool {
	public static void main(String[] args) {

		//Total = 3 Steps
		//1) Validation
		if(args.length<2)
		{
			System.out.println("Usage "+DeIdentifyDriver.class.getName()+" " +
					"/path/to/hdfs/input /path/to/hdfs/output");
			return;
		}
		//2) Loading configuration
		Configuration conf = new Configuration(Boolean.TRUE);
		//3) ToolRunner.run
		try {
			int i = ToolRunner.run(conf, new DeIdentifyDriver(), args);
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
		Job deIdentifyJob = Job.getInstance(conf, DeIdentifyDriver.class.getName());
		//step:3 : Setting the classpath to mapper on Datanode
		deIdentifyJob.setJarByClass(DeIdentifyDriver.class);
		
		//step:4 Setting the input
		final String input = args[0];
		final Path inputPath=new Path(input);
		TextInputFormat.addInputPath(deIdentifyJob, inputPath);
		deIdentifyJob.setInputFormatClass(TextInputFormat.class);
		
		//step:5 Setting the output
		
		final String output=args[1];
		final Path outputPath = new Path(output);
		TextOutputFormat.setOutputPath(deIdentifyJob, outputPath);
		deIdentifyJob.setOutputFormatClass(TextOutputFormat.class);
		//step:6 Setting the mapper
		
//		deIdentifyJob.setMapperClass(DeIdentifyMapper.class);
		
		//step:7 Setting the mapper output key and value class
		deIdentifyJob.setMapOutputKeyClass(Text.class);
		deIdentifyJob.setMapOutputValueClass(NullWritable.class);
		
		deIdentifyJob.setNumReduceTasks(0);
		
		//step:8: Setting the reducer
//		deIdentifyJob.setReducerClass(DeIdentifyReducer.class);
		//step:9: Setting the reducer output key and value class
//		deIdentifyJob.setOutputKeyClass(Text.class);
//		deIdentifyJob.setOutputValueClass(IntWritable.class);
		//step:10  : Trigger Method
		deIdentifyJob.waitForCompletion(Boolean.TRUE);
		return 0;
	}
}
