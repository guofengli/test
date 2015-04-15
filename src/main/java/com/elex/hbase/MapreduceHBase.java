package com.elex.hbase;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class MapreduceHBase {
	
	public static class PutMap extends Mapper<Object, Text, NullWritable, Put>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer valueTmp = new StringTokenizer(value.toString(), "\n");
			while(valueTmp.hasMoreTokens()){
				StringTokenizer wordUser = new StringTokenizer(valueTmp.nextToken());
				String word = wordUser.nextToken();
				String user = wordUser.nextToken();
				String tfidf = wordUser.nextToken();
				Put put = new Put(Bytes.toBytes(user));
				put.add(Bytes.toBytes("word_tfidf"), Bytes.toBytes(word), Bytes.toBytes(tfidf));
				context.write(NullWritable.get(), put);
			}
		}
	}
		
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "IndexBuilder");
		job.setJarByClass(MapreduceHBase.class);
		job.setMapperClass(PutMap.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Put.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		TableMapReduceUtil.initTableReducerJob("maptfidfTest", null, job);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
