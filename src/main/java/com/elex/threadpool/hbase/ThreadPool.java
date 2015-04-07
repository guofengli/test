package com.elex.threadpool.hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ThreadPool {
	public static String hdfsURL = "hdfs://10.1.20.241:8020";
	public static Configuration conf = null;
	public static FileSystem fs = null;
	
	public static void init() throws IOException{
		conf = new Configuration();
		fs = FileSystem.get(URI.create(hdfsURL), conf);
	}
	public static void main(String[] args) throws IOException, InterruptedException {
		long startTime = System.currentTimeMillis();	
		ExecutorService service = new ThreadPoolExecutor(8, 20, 10, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>());
			List<Future<String>> jobs = new ArrayList<Future<String>>();
			FileInputStream fis = null;
			conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(hdfsURL), conf);
			Path path = new Path("/tfidf/result/tfidf_2/part-r-00000");
			FSDataInputStream input = fs.open(path);
			byte[] bytes = new byte[1024];
			StringBuffer sb = new StringBuffer();
			int status = input.read(bytes) ;
			String tmpStr = null;
			ArrayList<String> lines = new ArrayList<String>();
			int putSize = 50000;
			int total = 0;
			while((tmpStr = input.readLine()) != null){
				lines.add(tmpStr);
				total++;
				if (lines.size() == putSize) {
					HBasePuts putter = new HBasePuts();
					putter.setList(lines);
					jobs.add(service.submit(putter));
					lines = new ArrayList<String>();
				}
				if(total %100000 == 0){
					System.out.println("total:" + total);
				}
			}
			if (lines.size() > 0) {
				HBasePuts putter = new HBasePuts();
				putter.setList(lines);
				jobs.add(service.submit(putter));
				lines = new ArrayList<String>();
			}
			
			for(Future<String> job : jobs){
	            System.out.println((System.currentTimeMillis() - startTime) + "ms ");
	        }		
	        service.shutdownNow();
		}

}
