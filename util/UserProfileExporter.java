package com.yeezhao.analyz.util;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.yeezhao.commons.hbase.HBaseUtil;
import com.yeezhao.commons.util.StringUtil;

public class UserProfileExporter {

	private HTable table = null;
	private final static byte[] COL_FMLY_CRAWL = Bytes.toBytes("crawl");
	private final static byte[] COL_FMLY_ANALYZ = Bytes.toBytes("analyz");

	public void setup(Configuration conf,String tableName) 
			throws IOException {		 
		table = new HTable(conf, tableName);	
	}
	public void cleanup() throws IOException{
		if(table != null)
			table.close();
	}
	
	public String getColumnValue(byte[] rowkey,byte[] family,byte[] qualifier){
		Get get = new Get(rowkey);
		get.addColumn(family, qualifier);
		try {
			Result rs = table.get(get);
			if(rs == null)
				return "";
			return Bytes.toString(rs.getValue(family, qualifier));
		} catch (IOException e) {			
			return null;
		}
	}

	public void getInfos(String rowkey,Map<String,String> infos){
		Get get = new Get(Bytes.toBytes(rowkey));
		get.addFamily(COL_FMLY_CRAWL);
		try {
			Result rs = table.get(get);
			if(rs == null)
				return;
			ResultParser.fillInfos(rs, infos);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void getTagsNTweets(String rowPrefix,List<String> tags,List<String> tweets){
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("fp"), Bytes.toBytes("content"));		
		scan.setStartRow( Bytes.toBytes(rowPrefix) );
		scan.setStopRow( Bytes.toBytes(HBaseUtil.getPrefixUpperBound(rowPrefix)));
		ResultScanner scanner = null;
		try {
			scanner = table.getScanner(scan);
			for(Result rs : scanner){
				String rowkey = Bytes.toString(rs.getRow());				
				if(rowkey.indexOf("|tag|") != -1){
					ResultParser.fillTags(rs, tags);
					if(tags.size()==0)
						break;
				}else if(rowkey.indexOf("|wb|") != -1){
					ResultParser.fillTweets(rs, tweets);
				}
			}
			scanner.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(scanner != null){
				scanner.close();
			}
		}
	}

	public boolean getAnalyz(String rowkey,Map<String,String> analyz){
		Get get = new Get(Bytes.toBytes( rowkey ));
		get.addFamily(COL_FMLY_ANALYZ);
		Result rs = null;
		try {
			rs = table.get(get);
			if(rs == null)
				return false;
			ResultParser.fillAnalyz(rs, analyz);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}		
		return true;
	}

	public boolean getInfosNAnalyz(String rowkey,Map<String,String> infos,Map<String,String> analyz){
		Get get = new Get(Bytes.toBytes( rowkey ));
		get.addFamily(COL_FMLY_CRAWL);
		get.addFamily(COL_FMLY_ANALYZ);
		Result rs = null;
		try {
			rs = table.get(get);
			if(rs == null)
				return false;
			ResultParser.fillAnalyz(rs, analyz);
			ResultParser.fillInfos(rs, infos);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}		
		return true;
	}
	
	//Get a user row's friend rows, given the input columns 
	public Result[] getFriendRows(Result usrRow, List<Pair<byte[], byte[]>> inputCols, boolean mutual ){
		String src = AppUtil.getUsrSrc(Bytes.toString(usrRow.getRow()));
		List<String> friends = new LinkedList<String>();				
		List<Get> getList = new LinkedList<Get>();
		ResultParser.fillFriends(usrRow, friends, mutual);
		if( friends.isEmpty() ) return new Result[]{}; 

		for( String friend: friends){
			Get get = new Get( Bytes.toBytes(src + StringUtil.DELIMIT_1ST + friend));
			HBaseUtil.addGetColumns(get, inputCols);
			getList.add(get);
		}
		
		try {
			return table.get(getList);
		} catch (IOException e) {
			e.printStackTrace();
			return new Result[]{};
		}
	}
}
