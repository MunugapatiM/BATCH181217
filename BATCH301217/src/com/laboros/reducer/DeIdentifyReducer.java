package com.laboros.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DeIdentifyReducer extends
		Reducer<Text, NullWritable, Text, NullWritable> {

	@Override
	protected void reduce(
			Text arg0,
			java.lang.Iterable<NullWritable> arg1,
			org.apache.hadoop.mapreduce.Reducer<Text, NullWritable, Text, NullWritable>.Context arg2)
			throws java.io.IOException, InterruptedException {
	};

}
