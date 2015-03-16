package com.elex.hbase;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class ReadFile {
	public static String tableName = "maphbase_test";
	public static Configuration conf = HBaseConfiguration.create();

	public static Date date = new Date();

	public static class ReadThread implements Callable {
		private File fileName;
		private long start;
		private long end;
		private String endStr = null;
		public ReadThread(File fileName, long start, long end) {
			this.fileName = fileName;
			this.start = start;
			this.end = end;
		}

		public String call() {
			RandomAccessFile randomFile = null;
			try {
				randomFile = new RandomAccessFile(fileName, "r");
				java.nio.channels.FileChannel channel = randomFile.getChannel();
				long readLength = 1024 * 1024;
				long postion = this.start;
				long length;
				List<Put> puts = new ArrayList<Put>();
				HTable table = new HTable(conf, tableName);
				table.setAutoFlush(false);
				table.setWriteBufferSize(6 * 1024 * 1024);
				while (true) {
					long channelLength = this.end - postion;
					if (channelLength <= 0) {
						break;
					}
					length = channelLength < readLength ? channelLength
							: readLength;
					java.nio.MappedByteBuffer buffer = channel.map(
							FileChannel.MapMode.READ_ONLY, postion, length);
					Charset charset = Charset.forName("utf-8");
					CharsetDecoder decoder = charset.newDecoder();
					CharBuffer charBuffer = decoder.decode(buffer);
					String userTFIDFtmp = null;
					if (this.endStr == null) {
						userTFIDFtmp = charBuffer.toString();
					} else {
						userTFIDFtmp = this.endStr + charBuffer.toString();
						this.endStr = null;
					}
					StringTokenizer userTFIDF = new StringTokenizer(
							userTFIDFtmp, "\n");
					int userCount = userTFIDF.countTokens();
					char endChar = userTFIDFtmp
							.charAt(userTFIDFtmp.length() - 1);
					while (userTFIDF.hasMoreTokens()) {
						if (endChar != '\n' && --userCount == 0) {
							this.endStr = userTFIDF.nextToken();
							break;
						}
						String str = userTFIDF.nextToken();
						StringTokenizer fraguser = new StringTokenizer(str);
						if (fraguser.countTokens() == 3) {
							String word = fraguser.nextToken();
							String user = fraguser.nextToken();
							String tfidf = fraguser.nextToken();
							 Put putData;
							 putData = new Put(Bytes.toBytes(user));
							 putData.add(Bytes.toBytes("word_tfidf"),
							 Bytes.toBytes(word), Bytes.toBytes(tfidf));
							 putData.setWriteToWAL(false);
							 puts.add(putData);
						}
						if(puts.size() == 10000){
							table.put(puts);
							puts = new ArrayList<Put>();
						}
					}
					postion += readLength + 1;
				}
				table.put(puts);
				table.close();
				System.out.println(new Date().getTime() - date.getTime());
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int threadSize = 3;
		File fileName = new File("E:\\work\\tfidf");
		long fileLength = fileName.length();
		long blockSize = fileLength / threadSize;
		long postion = 0;
		ExecutorService service = new ThreadPoolExecutor(5, 20, 60, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>());
		List<Future<String>> jobs = new ArrayList<Future<String>>();
		long start;
		long end;
		for (int i = 0; i < threadSize; i++) {
			start = (i == 0) ? 0 : (postion);
			end = getStartNum(fileName, (i + 1) * blockSize);
			postion = end;
			if (start < end) {		
				ReadThread thread = new ReadThread(fileName, start, end);
				jobs.add(service.submit(thread));
			}
		}
	}

	@SuppressWarnings("resource")
	public static long getStartNum(File fileName, long postion) {
		RandomAccessFile randomFile = null;
		try {
			randomFile = new RandomAccessFile(fileName, "r");
			long fileLength = fileName.length();
			if (fileLength <= postion) {
				return fileLength;
			}
			randomFile.seek(postion);
			String b = null;
			while ((b = randomFile.readLine()) != null) {
				postion += b.getBytes().length+1;
				break;
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return postion;
	}

}
