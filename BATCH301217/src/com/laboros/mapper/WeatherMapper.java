package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws java.io.IOException, InterruptedException {
		
		//key -- 0
		//value -- 27516 20140101  2.424 -156.61   71.32   -16.6   
		//-18.7   -17.7   -17.7     0.0     0.00 C   -17.8   -19.4   -18.7    83.8    73.5    80.8 -99.000 -99.000 -99.000 -99.000 -99.000 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0

		//step:1 : Convert into java data types
		
//		final long offset = key.get();
		
		final String iLine = value.toString();
		
		//step: 2 : Avoid Nullpointer exception check for empty
		
		if(StringUtils.isNotEmpty(iLine))
		{
			//Year Date Max_temp
			 
			//Get the date
			 String date = StringUtils.substring(iLine, 6, 14);
			 String year = StringUtils.substring(date, 0, 4);
			 String max_temp = StringUtils.substring(iLine, 38, 45);
			 
			 context.write(new Text(year),new Text(date+"\t"+max_temp));
		}
		
		
	};

}
