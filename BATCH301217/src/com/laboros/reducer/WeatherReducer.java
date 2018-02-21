package com.laboros.reducer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(
			Text key,
			java.lang.Iterable<Text> values,
			Context context)
			throws java.io.IOException, InterruptedException {
		//key -- 2017
		
		//values {date	max_temp
		
		//year -- key
		//max_temp <--- Compute
		//date <- maximum recorded temparature date
		
		float max_temparature_temp = Float.MIN_VALUE;
		 String max_temp_recorded_date="";
		
		for (Text input_date_temp : values) {
			String [] date_temp = StringUtils.split(input_date_temp.toString(), "\t");
			
			float input_temp = Float.parseFloat(date_temp[1].trim());
			if(max_temparature_temp<input_temp)
			{
				max_temparature_temp=input_temp;
				max_temp_recorded_date=date_temp[0];
			}
			
		}
		
		if(max_temparature_temp!=Float.MIN_VALUE && StringUtils.isNotEmpty(max_temp_recorded_date)){
			context.write(key, new Text(max_temp_recorded_date+"\t"+max_temparature_temp));
		}
	};

}
