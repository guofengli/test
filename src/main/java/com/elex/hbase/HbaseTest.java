package com.elex.hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTest {
	
	static Configuration conf = HBaseConfiguration.create();
	static HTable table = null;
	static Put putData = null;
	public static void creat(String tableName, String columnFamily) throws IOException{
	//	Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		if(admin.tableExists(tableName)){
			System.out.println("table Exists");
			System.exit(0);
		} 
		else{
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(tableDesc);
			System.out.println("create table success");
		}
	}
	
	public static void creat(String tableName, String []columnFamilies) throws IOException{
	//	Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		if(admin.tableExists(tableName)){
			System.out.println("table Exists");
			System.exit(0);
		} 
		else{
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for(String columnFamily:columnFamilies)
			tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(tableDesc);
			System.out.println("create table success");
		}
	}
	
	public static void put(String tableName, String row, String columnFamily, String column, String data) throws IOException{
		table = new HTable(conf, tableName);
		putData = new Put(Bytes.toBytes(row));
		putData.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
		table.put(putData);
//		table.close();
//		System.out.println("put data success");
	}
	
	//根据rowkey获取数据
	public static void get(String tableName, String row) throws IOException{
		HTable htable = new HTable(conf, tableName);
		Get getData = new Get(Bytes.toBytes(row));
		Result result = htable.get(getData);
		System.out.println("Get:" + result);
		htable.close();
	}
	
	//运用scan扫描全表
	public static void scan(String tableName) throws IOException{
		HTable htable = new HTable(conf, tableName);
		Scan scanData = new Scan();
		ResultScanner rs = htable.getScanner(scanData);
		for(Result result:rs){
			System.out.println(result);
		}
		htable.close();
	}
	
	//查询范围的keyrow
	public static void scan(String tableName, String column, String columnFamily) throws IOException{
		HTable htable = new HTable(conf,tableName);
		ResultScanner rs = htable.getScanner(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
		for(Result result:rs){
			System.out.println(result);
		}
		htable.close();
	}
	
	//对制定的表增加一个列族
	public static void addFamily(String tableName, String columnFamily) throws IOException{	
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.addColumn(tableName, new HColumnDescriptor(columnFamily));
	}
	
	//删除一个列族
	public static void deleteFamily(String tableName, String columnFamily) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		admin.deleteColumn(tableName, columnFamily);
		
	}
	
	
	public static void getFamilys(String tableName, String columnFamily) throws IOException{
		HTable htable = new HTable(conf, tableName);
		HTableDescriptor htd = new HTableDescriptor(tableName);
		HColumnDescriptor col = new HColumnDescriptor(columnFamily);
		System.out.println(col.getNameAsString());
//		htd.addFamily(col);	
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String tableName = "maphbase_test";
		String columnFamily="word_tfidf";
		String []columnFamilies = {"word_tfidf"};
		HbaseTest.creat(tableName, columnFamilies);
//		HbaseTest.put(tableName, "liguofeng", columnFamily, "c2", "hbase test three");
		
		
//		HbaseTest.get(tableName, "liguofeng");
//		HbaseTest.scan(tableName);
//		HbaseTest.scan(tableName,"c2",columnFamily);
//		HbaseTest.addFamily(tableName, columnFamily);
//		HbaseTest.deleteFamily(tableName, columnFamily);
//		HbaseTest.getFamilys(tableName, columnFamily);
//		readFileByLines("C:\\Users\\Administrator\\Downloads\\part-r-00000 (6)");
		}
	
	public static void readFileByLines(String fileName) {  
        File file = new File(fileName);  
        BufferedReader reader = null;  
        String tableName = "tfidf_test";
		String columnFamily="tfidf";
        try {  
            reader = new BufferedReader(new FileReader(file));  
            String tempString = null;  
            int line = 1;  
            System.out.println(new Date());
            while ((tempString = reader.readLine()) != null) {  
               // System.out.println("line " + line + ": " + tempString);  
            	StringTokenizer tmp = new StringTokenizer(tempString, "\t");
            	String word = tmp.nextToken();
            	StringTokenizer userTFIDF = new StringTokenizer(tmp.nextToken(), " ");
            	String user = userTFIDF.nextToken();
            	String tfidf = userTFIDF.nextToken();
            	HbaseTest.put(tableName, user, columnFamily, word, tfidf);
                line++;  
            }  
            System.out.println(new Date() + " " + line);
            reader.close();  
        } catch (IOException e) {  
            e.printStackTrace();  
        } finally {  
            if (reader != null) {  
                try {  
                    reader.close();  
                } catch (IOException e1) {  
                }  
            }  
        }  
    }
	
}
