package com.elex.hbase;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountHbase {

	public static class CountHbaseMap extends Mapper<Object, Text, Text, Text>{
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
	
	public static class CountHbaseReduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context){
			
		}
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	}

}
