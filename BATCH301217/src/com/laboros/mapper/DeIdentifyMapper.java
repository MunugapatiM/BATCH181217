package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeIdentifyMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {
	
	private int[] deIdentifyColumns = {4,5,6,8};
	
	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws java.io.IOException, InterruptedException {
		
		//key -- 0
		//value ---11111,bbb1,12/10/1950,1234567890,bbb1@xxx.com,1111111111,M,Diabetes,78
		
//		long lineOffset = key.get();
		
		final String iLine = value.toString();
		//Check null
		if(StringUtils.isNotEmpty(iLine))
		{
			final String[] columns = StringUtils.splitPreserveAllTokens(iLine, ",");
			
			int iLength = columns.length;
			
			StringBuffer encryptedData = new StringBuffer();
			for (int iColIdx=0;iColIdx<iLength;iColIdx++)
			{
				//First Point check which column(iColIdx) need to encrpyt
				if(checkColumnNeedToEncrpt(iColIdx))
				{
					encryptedData.append(encrypt(columns[iColIdx]));
				}else{
					encryptedData.append(columns[iColIdx]);
				}
				encryptedData.append(",");
			}
			
			context.write(new Text(encryptedData.toString()), NullWritable.get());
			
		}
		
		
 
	};
	
	private String encrypt(final String column)
	{
		
		return "XXXXAESXX-XXXXX-XXXX";
	}
	
	private boolean checkColumnNeedToEncrpt(final int columnIndex)
	{
		for (int deIdentifyColumn : deIdentifyColumns) {
			if(columnIndex+1==deIdentifyColumn)
			{
				return Boolean.TRUE;
			}
		}
		return Boolean.FALSE;
	}
}
