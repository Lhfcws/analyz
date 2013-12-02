package com.yeezhao.analyz.util;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.yeezhao.commons.util.StringUtil;

public class ResultParser {
	
	
	public static String getUid(String rowkey){
		String[] segs = rowkey.split("\\|");
		return segs.length == 2 ? segs[1] : null;
	}

	public static void fillInfos(Result rs,Map<String,String> infos){
		NavigableMap<byte[],byte[]> map = rs.getFamilyMap(AppConsts.COL_FMLY_CRAWL);
		if(map == null)
			return;
		for(Entry<byte[],byte[]> e : map.entrySet()){
			infos.put(Bytes.toString(e.getKey()), Bytes.toString(e.getValue()));
		}
	}
	
	public static void fillAnalyz(Result rs,Map<String,String> analyz){
		NavigableMap<byte[],byte[]> map = rs.getFamilyMap(AppConsts.COL_FMLY_ANALYZ);
		if(map == null)
			return;
		for(Entry<byte[],byte[]> e : map.entrySet()){
			analyz.put(Bytes.toString(e.getKey()), Bytes.toString(e.getValue()));
		}
	}

	public static void fillTags(Result rs,List<String> tags){
		for(KeyValue kv : rs.raw()){
			if(AppConsts.COL_CONTENT.equals(kv.getQualifier())){
				String content = Bytes.toString( kv.getValue() );
				if(content!=null && content.length()>0)
					for(String t : content.split(StringUtil.STR_DELIMIT_1ST)){
						t = t.trim();
						if(t.length()>0)
							tags.add(t);
					}
			}
		}
	}
	
	public static void fillTweets(Result rs,List<String> tweets){		
		for(KeyValue kv : rs.raw()){			
			if(AppConsts.COL_CONTENT.equals(kv.getQualifier())){
				tweets.add(Bytes.toString( kv.getValue() ) );
			}
		}
	}
	
	public static void fillFriends(Result rs,List<String> friends){
		NavigableMap<byte[],byte[]> map = rs.getFamilyMap(AppConsts.COL_FMLY_FRIEND);
		if(map == null)
			return;
		for(byte[] key : map.keySet()){
			String friend = Bytes.toString(key);
			if(friend != null && friend.length() > 0)
				friends.add(friend);
		}
	}
	
	public static void fillFriends(Result rs,List<String> friends,boolean mutual){
		NavigableMap<byte[],byte[]> map = rs.getFamilyMap(AppConsts.COL_FMLY_FRIEND);
		if(map == null)
			return;
		for(Entry<byte[],byte[]> e : map.entrySet()){
			String friend = Bytes.toString( e.getKey() );
			if(friend == null || friend.length()==0)
				continue;			
			if(mutual && e.getValue().length > 0)
				friends.add(friend);
			else if(!mutual && e.getValue().length == 0)
				friends.add(friend);
		}
	}
	
	
}
