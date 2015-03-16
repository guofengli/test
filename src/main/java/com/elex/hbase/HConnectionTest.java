package com.elex.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

public class HConnectionTest {
	private HConnection conn = null;
	private Configuration conf = null;

	/**
	 * creating connection object for HBase, the pool size is 256 by default.
	 */
	public void QueryUtility() {
		this.conf = HBaseConfiguration.create();
		try {
			this.conn = HConnectionManager.createConnection(this.conf);
		} catch (IOException e) {
			System.out.println("connection failed, process will exit");
			e.printStackTrace();
			System.exit(1);
		}
	}

	public void release()
	{
		try {
			this.conn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void queryOneRecord(String strRowkey)
	{
		try {
			/**
			 * the HTable object can also be set the class member.
			 */
			HTable table = (HTable)this.conn.getTable("testtable");
			
	        Get get = new Get(strRowkey.getBytes());
	        Result rs = table.get(get);
	        
	        String strRes = "this row " + strRowkey + " has " + rs.size()+ " versions";
	        System.out.println(strRes);
	        
	        table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}