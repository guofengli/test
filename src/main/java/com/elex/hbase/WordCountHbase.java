package com.elex.hbase;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class WordCountHbase {

	public static class TFIDFMap extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer valueTmp = new StringTokenizer(value.toString(), "\n");
			while(valueTmp.hasMoreTokens()){
				StringTokenizer wordUser = new StringTokenizer(valueTmp.nextToken());
				String word = wordUser.nextToken();
				String user = wordUser.nextToken();
				String tfidf = wordUser.nextToken();
				Text outputKey = new Text();
				Text outputValue = new Text();
				outputKey.set(user);
				outputValue.set(word + "\t" + tfidf);
				context.write(outputKey, outputValue);
			}
		}
	}
	
	 public static class TFIDFReduce extends TableReducer<Text, Text, NullWritable> {   
  
   public void reduce(Text key, Iterable<Text> values,   
       Context context) throws IOException, InterruptedException {   
//	   ArrayList<Put> puts = new ArrayList<Put>(); 
	   for(Text value:values){
		   StringTokenizer wordTFIDF = new StringTokenizer(value.toString());
		   String word = wordTFIDF.nextToken();
		   String tfidf = wordTFIDF.nextToken();
		   Put put = new Put(Bytes.toBytes(key.toString()));
		   put.add(Bytes.toBytes("word_tfidf"), Bytes.toBytes(word), Bytes.toBytes(tfidf));
//		   puts.add(put);
		   context.write(NullWritable.get(), put);
	   }
//	   context.write(NullWritable.get(), puts);   
   }   
 }   
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		String tableName = "maptfidfTest";
		Configuration conf = new Configuration();   
	    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);   
	   
	    Job job = Job.getInstance(conf, "mapredeuce hbase job");   
	    job.setJarByClass(WordCountHbase.class);   
	    
	    
	    // 设置 Map 和 Reduce 处理类   
	    job.setMapperClass(TFIDFMap.class);   
	    job.setReducerClass(TFIDFReduce.class);   
	   
	    // 设置输出类型   
	    job.setMapOutputKeyClass(Text.class);   
	    job.setMapOutputValueClass(Text.class);   
	   
	    // 设置输入和输出格式   
	    job.setInputFormatClass(TextInputFormat.class);   
	    job.setOutputFormatClass(TableOutputFormat.class);   
	   
	    // 设置输入目录   
	    FileInputFormat.addInputPath(job, new Path("/tfidf/result/tfidf_1/part-r-00000"));   
	    System.exit(job.waitForCompletion(true) ? 0 : 1);      
	}
}
