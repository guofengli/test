package com.elex.threadpool.hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class ThreadReadDemo {
	public static String tableName = "maphbase_test";
	public static Configuration conf = HBaseConfiguration.create();
	public static Date date1;

	public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {	
		 date1 = new Date();
		 for (int i = 0; i < 10; i++) {
		 Thread t1 = new Thread(new MultiThread());
		 t1.start();
		 }
	}
	static class MultiThread implements Runnable {
		private static BufferedReader br = null;
		private List<String> list;
		static {
			try {
				br = new BufferedReader(new FileReader("E:\\work\\tfidf"));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		public void run() {
			String line = null;
			int count = 0;
			while (true) {
				this.list = new ArrayList<String>();
				synchronized (br) {
					try {
						while ((line = br.readLine()) != null) {
							if (count < 50000) {
								list.add(line);
								count++;
							} else {
								list.add(line);
								count = 0;
								break;
							}
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				try {
					display(this.list);
				} catch (IOException e) {
					e.printStackTrace();
				}
				if (line == null)
					break;
			}
		}

		public void display(List<String> list) throws IOException {
			HTable table = new HTable(conf, tableName);
			table.setWriteBufferSize(6 * 1024 * 1024);
			table.setAutoFlush(false);
			List<Put> puts = new ArrayList<Put>();
			for (String str : list) {
				StringTokenizer tfidftmp = new StringTokenizer(str);
				String word = tfidftmp.nextToken();
				String user = tfidftmp.nextToken();
				String tfidf = tfidftmp.nextToken();
				Put putData;
				putData = new Put(Bytes.toBytes(user));
				putData.add(Bytes.toBytes("word_tfidf"), Bytes.toBytes(word),
						Bytes.toBytes(tfidf));
				putData.setWriteToWAL(false);
				puts.add(putData);
			}
			table.put(puts);
			table.close();
			System.out.println(new Date().getTime() - date1.getTime());
		}
	}
}