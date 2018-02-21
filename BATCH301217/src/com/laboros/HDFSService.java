package com.laboros;

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//hadoop fs -put WordCount.txt /user/trainings

//java -classpath/-cp HadoopJars/* com.laboros.HDFSService WordCount.txt /user/trainings

public class HDFSService extends Configured implements Tool {

	public static void main(String[] args) {

		// validations
		if (args.length < 2) {
			System.out
					.println("Usage: java "+HDFSService.class.getName()+" " +
							"/path/to/edge/node/local/file /path/to/hdfs/dest/dir");
			return;
		}
		// hadoop -- utility is performing 4 steps
		// 1) Load the configuration
		Configuration conf = new Configuration(Boolean.TRUE);// LOAD
																// core-default.xml,
																// core-site.xml

		conf.set("fs.defaultFS", "hdfs://localhost:8020");
		// 2) Setting the classpath :: Runtime -classpath hadoopjars
		// 3) Identify the COMMAND = fs -- Ignore
		// 4) Identify the class and executing it -- Ignore
		// org.apache.hadoop.fs.FsShell --- main() --- ToolRunner.run

		try {
			int i = ToolRunner.run(conf, new HDFSService(), args);
			if (i == 0) {
				System.out.println("SUCCESS");
			} else {
				System.out.println("FAILURE");
			}
		} catch (Exception e) {
			System.out.println("FAILURE");
			e.printStackTrace();
		}
		// -put : Identify the Method : copyFromLocal
	}

	@Override
	public int run(String[] args) throws Exception 
	{
		Configuration conf = super.getConf();
		//Writing Dataset == Create Metadata + Add data
		
		//creating metadata = creating an empty file
		//Create an empty file
		
		//Get the FileName, Get the Destination directory from command line arguments
		
		final String fileName = args[0]; //WordCount.txt
		final String destDir = args[1];//user/trainings
		
		//Convert into PATH : HDFS understand every file and directory as URI

		//	hdfs://localhost:8020/user/trainings/WordCount.txt
		Path hdfsDestFileNamePath= new Path(destDir, fileName);  
		
		//Create the FileSystem object
		
		FileSystem hdfs = FileSystem.get(conf);
		
		FSDataOutputStream fsdos=hdfs.create(hdfsDestFileNamePath);
		
		//Add the Data
//			1) Split Data into Blocks
		//Step-1 Read input through InputStream
		InputStream is = new FileInputStream(fileName);
//			2) Identify the DataNode for a block
//			3) Writing datablock to datanode
//			4) Writing replication	--- HDFS Property
//			5) Syn the metadata with NN
//			6) Handling Failure   -- HDFS Property

		IOUtils.copyBytes(is, fsdos, conf, Boolean.TRUE);
		return 0;
	}

}
