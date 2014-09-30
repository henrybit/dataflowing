package com.meetyou.core.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import classes.org.apache.hadoop.io.Text;

/**
 * 读取HDFS文件<br>
 * example:hdfs://zoo38:9000/data/kafka
 * @author henrybit
 * @version 1.0
 * @since 2014/09/28
 */
public class HDFSReader {
	
	public static void main(String[] args) throws Exception {
		String uri = "hdfs://zoo38:9000/data/kafka/FlumeData.1411893286541";
		//uri = "/data/kafka/FlumeData.1411893286541";
		 //读取hadoop文件系统的配置 
        Configuration conf = new Configuration(); 
        conf.set("hadoop.job.ugi", "hadoop");
		FileSystem fs = FileSystem.get(URI.create(uri),conf);
		SequenceFile.Reader sequenceFileReader = new SequenceFile.Reader(fs, new Path(uri), conf);
		Text key = new Text();
		Text value = new Text();
		while(sequenceFileReader.next(key, value)) {
			 System.out.println(key);  
			 System.out.println(value); 
		}
		
		sequenceFileReader.close();
	}
}
