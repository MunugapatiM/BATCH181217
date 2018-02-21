package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {


	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws java.io.IOException, InterruptedException {
		
		//key -- 0
		//value -- DEER RIVER RIVER
		
		//Step:1 Convert into String -- UTILITIES are on STRING
		final String iLine = value.toString();
		
		//Step:2: Check for Empty or null
		
		if(StringUtils.isNotEmpty(iLine))
		{
			//Step: 3 : Tokenize
			final String[] words = StringUtils.splitPreserveAllTokens(iLine, " ");
			//words[0]= DEER
			//words[1]= RIVER
			//words[2]= RIVER
			
			for (String word : words) {
				context.write(new Text(word), new IntWritable(1));
			}
			
			
		}
		
	};
}
