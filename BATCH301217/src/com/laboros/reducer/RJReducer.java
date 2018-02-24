package com.laboros.reducer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RJReducer extends
		Reducer<Text, Text, Text, Text> {

	protected void reduce(
			Text key,
			java.lang.Iterable<Text> values,
			Context context)
			throws java.io.IOException, InterruptedException 
			{
		
		//key -- 4007024
		//value -- {040.34-product, kristiana-chung, 343.34,4343.3}
		
		String name="";
		int count=0;
		float totalAmount =0;
		
		for (Text identifier_values : values) {
			String[] tokens = StringUtils.split(identifier_values.toString(), "\t");
			if(StringUtils.equalsIgnoreCase(tokens[0], "TXNS"))
			{
				count++;
				totalAmount +=Float.parseFloat(tokens[1]);
			}else{
				name = tokens[1];
			}
		}
		if(StringUtils.isNotEmpty(name)){
		context.write(new Text(name), new Text(count+"\t"+totalAmount));
		}	
		};

}
