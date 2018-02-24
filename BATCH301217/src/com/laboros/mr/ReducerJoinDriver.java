package com.laboros.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.CustMapper;
import com.laboros.mapper.TxnMapper;
import com.laboros.reducer.RJReducer;


//yarn jar client.jar com.laboros.mr.ReducerJoinDriver cust txn output
public class ReducerJoinDriver extends Configured implements Tool {
	public static void main(String[] args) {

		//Total = 3 Steps
		//1) Validation
		if(args.length<3)
		{
			System.out.println("Usage "+ReducerJoinDriver.class.getName()+" " +
					"/path/to/hdfs/cust/input /path/to/hdfs/txn/input /path/to/hdfs/output");
			return;
		}
		//2) Loading configuration
		Configuration conf = new Configuration(Boolean.TRUE);
		//3) ToolRunner.run
		try {
			int i = ToolRunner.run(conf, new ReducerJoinDriver(), args);
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
		Job rjJob = Job.getInstance(conf, ReducerJoinDriver.class.getName());
		//step:3 : Setting the classpath to mapper on Datanode
		rjJob.setJarByClass(ReducerJoinDriver.class);
		
		//step:4 Setting the input
		
		//step: 4-a : Set the customer
		final String custInput = args[0];
		final Path custInputPath = new Path(custInput);
		MultipleInputs.addInputPath(rjJob, custInputPath, TextInputFormat.class, CustMapper.class);
		
		
		//step: 4-b Set the Reducer
		final String txnInput = args[1];
		final Path txnInputPath = new Path(txnInput);
		MultipleInputs.addInputPath(rjJob, txnInputPath, TextInputFormat.class, TxnMapper.class);
		
		
		//step:5 Setting the output
		
		final String output=args[2];
		final Path outputPath = new Path(output);
		TextOutputFormat.setOutputPath(rjJob, outputPath);
		rjJob.setOutputFormatClass(TextOutputFormat.class);
		
		//step:7 Setting the mapper output key and value class
		rjJob.setMapOutputKeyClass(Text.class);
		rjJob.setMapOutputValueClass(Text.class);
		
		//step:8: Setting the reducer
		rjJob.setReducerClass(RJReducer.class);
		//step:9: Setting the reducer output key and value class
		rjJob.setOutputKeyClass(Text.class);
		rjJob.setOutputValueClass(IntWritable.class);
		//step:10  : Trigger Method
		rjJob.waitForCompletion(Boolean.TRUE);
		return 0;
	}
}
