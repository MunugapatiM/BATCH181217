package com.laboros.mr.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartions) 
	{
		if(key.toString().equalsIgnoreCase("hadoop"))
		{
			return 0;//reducer -0
		}
		if(key.toString().equalsIgnoreCase("data"))
		{
			return 1;//reducer -1
		}
		
		return 2;
	}

}
