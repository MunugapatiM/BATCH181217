package com.laboros.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	protected void reduce(
			Text key,
			java.lang.Iterable<IntWritable> values,
			Context context)
			throws java.io.IOException, InterruptedException 
			{
		//key -- CAT
		//values -- {1,1,1,1}
		int sum=0;
		for (IntWritable intWritable : values) {
			sum = sum+intWritable.get();
		}
		context.write(key, new IntWritable(sum));
	};

}
