package com.elex.threadpool.hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

public class ThreadPool {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		long startTime = System.currentTimeMillis();
		File file = new File("E:\\work\\tfidf");
		ExecutorService service = new ThreadPoolExecutor(4, 10, 60, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>());
		List<Future<String>> jobs = new ArrayList<Future<String>>();
		FileInputStream fis = null;
		BufferedReader reader = null;
		fis = new FileInputStream(file);
		reader = new BufferedReader(new InputStreamReader(fis));
		String line;
		int putSize = 10000;
		ArrayList<String> lines = new ArrayList<String>();
		while ((line = reader.readLine()) != null) {
			lines.add(line);
			if (lines.size() == putSize) {
				HBasePuts putter = new HBasePuts();
				putter.setList(lines);
				jobs.add(service.submit(putter));
				lines = new ArrayList<String>();
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
