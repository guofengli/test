package com.elex.threadpool.hbase;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBasePuts implements Callable<String>{
	public static String tableName = "maphbase_test";
	public static Configuration conf = HBaseConfiguration.create();
	private ArrayList<String> list;
	public ArrayList<String> getList() {
		return list;
	}

	public void setList(ArrayList<String> lines) {
		this.list = lines;
	}

	public String call() throws Exception {
		// TODO Auto-generated method stub
		HTable table = new HTable(conf,tableName);
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
			putData.add(Bytes.toBytes("word_tfidf"), Bytes.toBytes(word), Bytes.toBytes(tfidf));
			putData.setWriteToWAL(false);
			puts.add(putData);
		}
		table.put(puts);
		table.flushCommits();
		table.close();
		System.gc();
		return null;
	}

}
