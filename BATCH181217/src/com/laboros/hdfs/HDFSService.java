package com.laboros.hdfs;

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//hadoop fs -put WordCount.txt /user/trainings
//java org.apache.hadoop.fs.FsShell -put WordCount.txt /user/training

//java -cp CLASSPATH com.laboros.hdfs.HDFSService -Ddfs.blocksize=256MB 
// -Ddfs.replication=4 WordCount.txt /user/training

public class HDFSService extends Configured implements Tool
{
	
	public static void main(String[] args) 
	{
		//validation
		
		if(args.length<2)
		{
			System.out.println("Usage HDFSService : [configuration] /path/to/edgenode/local/file" +
					" /hdfs/destination/directory");
			
			return;
		}
		//loading configurations
		Configuration conf = new Configuration(Boolean.TRUE);
		conf.set("fs.defaultFS", "hdfs://localhost:8020");
		try {
			int i = ToolRunner.run(conf, new HDFSService(), args);
			if(i==0)
			{
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
		
		//Get the configuration
		final Configuration conf = super.getConf();
		//File Write = Writing metadata + Add Data
		
		//Step:1 : Writing Metadata
		//Step:a : Create an Empty File
		// Empty  File = Destination + InputFile
		
		final String inputFile = getFileName(args[0]); //WordCount.txt /home/trainings/...../WordCount.txt
		final String destination = args[1];
		// Convert into URI USING PATH
		
		final Path destinationPathWithFileName = new Path(destination, inputFile);
		
		// Create Connectivity creating FileSystem
		FileSystem hdfs = FileSystem.get(conf);
		//Invoke Create
		FSDataOutputStream fsdos = hdfs.create(destinationPathWithFileName);
		//step-2: Add Data
		//Split Data into data blocks
				//Get the inputstream  -- Call Open Method
		
		InputStream is = new FileInputStream(inputFile);
				//read the data into Byte[] 
				//Submit the FsDataOutputStream
		//Identify the DATANODES for a Data Block
		//Writing data to DATANODE
		//Writing the replication
		//Syncing METADATA with NameNode
		//Handling Faliure --- HDFS
		
		IOUtils.copyBytes(is, fsdos, conf, Boolean.TRUE);
		
		
		return 0;
	}

	private String getFileName(String fileNameWithLocation) {
		
		if(StringUtils.startsWith(fileNameWithLocation, "/"));
		{
			//REMOVE Complete Path and get the fileName
			// /home/trainings/SAIWS/BATCH181217/Datasets/WordCount.txt
			//fileName = WordCount.txt
		}
		return fileNameWithLocation;
	}

}
