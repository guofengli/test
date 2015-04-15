package com.elex.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

public class MyRowKeyFilter extends FilterBase {
	private byte[] value = null;
	public Map<Object, Object> map = new HashMap<Object, Object>();
	public MyRowKeyFilter(){
		super();
	}
	public MyRowKeyFilter(byte[] value){
		this.value = value;
	}
	
	@Override
	public boolean filterRow() throws IOException {
		// TODO Auto-generated method stub
		return super.filterRow();
	}
	
	public ReturnCode filterKeyValue(KeyValue  ignored) throws IOException {
		// TODO Auto-generated method stub
		return super.filterKeyValue(ignored);
	}
	@Override
	public boolean filterRowKey(byte[] buffer, int offset, int length)
			throws IOException {
		// TODO Auto-generated method stub
		return super.filterRowKey(buffer, offset, length);
	}

	@Override
	public void reset() throws IOException {
		// TODO Auto-generated method stub
		super.reset();
	}

	public void write(DataOutput dataOutput) throws IOException {
		Bytes.writeByteArray(dataOutput, this.value);
	}

	public void readFields(DataInput dataInput) throws IOException {
		this.value = Bytes.readByteArray(dataInput);

		String string = new String(this.value);
		String[] strs = string.split(",");
		for (String str : strs) {
			map.put(str, str);
		}
	}
}
