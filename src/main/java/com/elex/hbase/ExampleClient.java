package com.elex.hbase;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class ExampleClient {
	public static void main(String[] args) throws IOException {
		
		  Configuration conf = HBaseConfiguration.create();
		  HBaseAdmin admin = new HBaseAdmin(conf);
		  HTableDescriptor tableDescriptor = admin.getTableDescriptor(Bytes.toBytes("tfidf_test"));
		  byte[] name = tableDescriptor.getName();
		  System.out.println("result:");

		  System.out.println("table name: "+ new String(name));
		  HColumnDescriptor[] columnFamilies = tableDescriptor
				  .getColumnFamilies();
		  for(HColumnDescriptor d : columnFamilies){
			  System.out.println("column Families: "+ d.getNameAsString());
			  }
	    }
}