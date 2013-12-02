package com.yeezhao.analyz.group;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.yeezhao.commons.hbase.HBaseUtil;
import com.yeezhao.commons.util.StringUtil;

public class AnalyzSinaUserPref {

	static class EntryValueComparator implements Comparator<Entry<String,Double>>{
		@Override
		public int compare(Entry<String, Double> o1,
				Entry<String, Double> o2) {				
			return - o1.getValue().compareTo(o2.getValue());
		}
	}
	static class WeightedTag{
		private String tag;
		private double weight;

		public WeightedTag(String tag,double weight){
			this.tag = tag;
			this.weight = weight;
		}

		public String getTag(){
			return tag;
		}

		public double getWeigth(){
			return weight;
		}		

		public String toString(){
			return String.format("%.5f\t%s\t", weight, tag);
		}
	}

	private final int occurenceCount;
	private final int justifiedCount;
	private final boolean oneway;
	private final int minFans;
	private final int maxFans;
	private final Map<String,String> nickmap = new HashMap<String,String>();

	public AnalyzSinaUserPref(int occurenceCount,int justifiedCount,boolean oneway,int minFans,int maxFans){
		this.occurenceCount = occurenceCount;
		this.justifiedCount = justifiedCount;
		this.oneway = oneway;
		this.minFans = minFans;
		this.maxFans = maxFans;
		this.init();
	}
	private Configuration conf = null;
	private final static String TABLE_NAME = "yeezhao.user.profile";
	private HTable table = null;
	private void init(){
		conf = HBaseConfiguration.create();
		try {
			table = new HTable(conf, TABLE_NAME);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	}
	@Override
	public void finalize(){
		try {
			if(table != null)
				table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public List<WeightedTag> analyzUser(String uid,Map<String,Integer> tagOccurences,Map<String,Map<String,Double>> tagFollows) 
			throws IOException {
		Map<String,String> infos = new HashMap<String,String>();
		List<String> follows = new ArrayList<String>();
		Get get = new Get(Bytes.toBytes("sn|" + uid));
		get.addFamily(Bytes.toBytes("crawl"));
		get.addFamily(Bytes.toBytes("friend"));
		getCrawl(get,infos,follows);
		int missdata = 0;
		if(infos.size()==0){
			System.out.println("No data for user " + uid);
			System.exit(1);
		}
		if(follows.size()==0){
			System.out.println("No followees for user " + uid);
			System.exit(1);
		}
		if(infos.containsKey("name")){
			String name = infos.get("name");
			if(name.length()>0)
				nickmap.put(uid, name);
		}

		Map<String,Double> tagWeights = new HashMap<String,Double>();

		List<String> candidates = new ArrayList<String>(follows);

		int numOnewayFollowees = 0;
		int numOnewayFolloweesWithTag = 0;
		//用户自己的标签也并入统计
		if(addTagWeight(uid,tagOccurences,tagWeights,tagFollows)){
			numOnewayFolloweesWithTag++;
		}

		for(String id : candidates){
			infos.clear();
			follows.clear();
			get = new Get(Bytes.toBytes("sn|" + id));
			get.addFamily(Bytes.toBytes("crawl"));
			if(oneway)
				get.addFamily(Bytes.toBytes("friend"));
			getCrawl(get,infos,follows);
			if(infos.size()==0 || follows.size()==0)
				missdata++;
			if(infos.containsKey("name")){
				String name = infos.get("name");
				if(name.length()>0)
					nickmap.put(id, name);
			}
			String followersCount = infos.get("followers_count");

			if(followersCount == null)
				continue;
			int count = Integer.parseInt(followersCount);

			//忽略大号粉丝微博
			if(count < minFans || count > maxFans)
				continue;

			boolean isfriend = false;
			if(oneway){
				for(String f : follows)
					if(uid.equals(f)){
						isfriend = true;
						break;
					}
			}
			if(isfriend)
				continue;
			numOnewayFollowees++;
			if(addTagWeight(id,tagOccurences,tagWeights,tagFollows))
				numOnewayFolloweesWithTag++;
		}
		System.out.println("#Details:");
		System.out.println("all_followees=" + candidates.size());
		System.out.println("followees_missdata=" + missdata);
		System.out.println("check_followees=" + numOnewayFollowees);
		System.out.println("check_followees_withtag=" + numOnewayFolloweesWithTag);
		int topk = 0;
		for(Entry<String,Integer> e : tagOccurences.entrySet())
			if(e.getValue() >= justifiedCount){
				topk++;
			}
		List<Entry<String,Double>> list = new ArrayList<Entry<String,Double>>(tagWeights.entrySet());
		Collections.sort(list, new EntryValueComparator());
		List<WeightedTag> result = new ArrayList<WeightedTag>(topk);
		for(int i=0;i<topk;++i){
			if(i >= list.size())
				break;
			Entry<String,Double> e = list.get(i);
			if(tagOccurences.get(e.getKey()) < occurenceCount)
				continue;
			result.add( new WeightedTag(e.getKey(),e.getValue()) );
		}
		return result;
	}
	/**
	 * 计算单向关注用户标签的权值，累加到tagWeights中，并统计每个标签出现的频数。
	 * @param id
	 * @param tagOccurences
	 * @param tagWeights
	 */
	private boolean addTagWeight(String id,Map<String,Integer> tagOccurences,
			Map<String,Double> tagWeights,Map<String,Map<String,Double>> tagFollows){		
		List<WeightedTag> wts = getWeightedTags(id);		
		for(WeightedTag wt : wts){
			if(!tagOccurences.containsKey(wt.getTag())){
				tagOccurences.put(wt.getTag(), 1);
				tagWeights.put(wt.getTag(), wt.getWeigth());
				Map<String,Double> follows = new HashMap<String,Double>();
				tagFollows.put(wt.getTag(), follows);
				follows.put(id, wt.getWeigth());
			}else{
				tagOccurences.put(wt.getTag(), 1 + tagOccurences.get(wt.getTag()));
				tagWeights.put(wt.getTag(), wt.getWeigth() + tagWeights.get(wt.getTag()));
				tagFollows.get(wt.getTag()).put(id, wt.getWeigth());
			}
		}
		return wts.size()>0;
	}

	/**
	 * 从HBase表中抓取指定用户标签以及微博，并计算用户的每个标签权值。
	 * 权值计算方式如下：
	 * 1. 每个标签默认值为1；
	 * 2. 遍历所有微博，如果微博包含某个标签，则该标签值加1；
	 * 3. 归一化所有标签的值。
	 * @param uid
	 * @return
	 */
	public List<WeightedTag> getWeightedTags(String uid){
		List<WeightedTag> result = new ArrayList<WeightedTag>();
		List<String> tags = new ArrayList<String>();
		List<String> tweets = new ArrayList<String>();
		long start = System.currentTimeMillis();
		System.out.print("Scaning sn|" + uid );
		getTagsNTweets(uid,tags,tweets);
		System.out.println(String.format(".\t[%dms,%dwb,%dtag]",(System.currentTimeMillis()-start),tweets.size(),tags.size()));
		if(tags.size()==0)
			return result;
		Map<String,Integer> map = new HashMap<String,Integer>();
		//start = System.currentTimeMillis();
		//System.out.print("Calculating tag weights");
		int covered = 0;
		String name = nickmap.get(uid);
		if(name==null || name.length()==0)
			name = "@";
		tags.remove(name);
		for(String tag : tags)
			map.put(tag, 1);
		/*for(int i=0;i<tweets.size();i++){
			tweets.set(i, tweets.get(i).replaceAll(name, ""));
		}*/
		for(String tweet : tweets){
			boolean putted = false;
			for(String tag : tags){
				if(tweet.indexOf(tag) != -1){
					map.put(tag, 1 + map.get(tag));
					putted = true;
				}
			}
			if(putted)
				covered++;
		}
		int sum = 0;
		for(int count : map.values())
			sum += count;
		double confidence = 1.0 * covered / tweets.size();
		for(Entry<String,Integer> e : map.entrySet()){
			result.add( new WeightedTag(e.getKey(), confidence * e.getValue() / sum ));
		}
		//System.out.println(".[" + (System.currentTimeMillis()-start) + "ms]");
		return result;
	}

	/**
	 * 获取用户在crawl列表中所有资料，包括基本信息以及关注列表ID
	 * @param id
	 * @param infos
	 * @param follows
	 * @throws IOException
	 */
	private void getCrawl(Get get,Map<String,String> infos,List<String> follows)
			throws IOException {
		long start = System.currentTimeMillis();
		System.out.print("Getting " + new String(get.getRow()) );
		Result rs = table.get(get);
		if(rs == null)
			return;
		String family,qualifier;
		for (KeyValue kv : rs.raw()) {
			family = new String(kv.getFamily());
			qualifier = new String(kv.getQualifier());
			if("crawl".equals(family)){
				infos.put(qualifier, new String(kv.getValue()));
			}else if("friend".equals(family)){
				follows.add(qualifier);
			}
		}
		System.out.println(".\t[" + (System.currentTimeMillis()-start) + "ms]");
	}

	/**
	 * 获取用户的标签以及微博消息
	 * @param uid
	 * @param tags
	 * @param tweets
	 */
	private void getTagsNTweets(String uid,List<String> tags,List<String> tweets){
		//System.out.println("Scaning " + uid);
		Scan scan = new Scan();
		scan.addFamily("fp".getBytes());
		String rowPrefix = "sn|" + uid + "|";
		scan.setStartRow( Bytes.toBytes(rowPrefix) );
		scan.setStopRow( Bytes.toBytes(HBaseUtil.getPrefixUpperBound(rowPrefix)));
		try {
			ResultScanner scanner = table.getScanner(scan);
			for(Result rs : scanner){
				String rowkey = new String(rs.getRow());				
				if(rowkey.matches("^sn\\|" + uid + "\\|tag\\|")){
					for(KeyValue kv : rs.raw()){
						if("content".equals(new String(kv.getQualifier()))){
							String content = new String( kv.getValue() );
							if(content!=null && content.length()>0)
								for(String t : content.toLowerCase().split("\\|")){
									if(!tags.contains(t))
										tags.add(t);
								}
						}			
					}
					if(tags.size()==0)
						break;
				}else if(rowkey.matches("^sn\\|" + uid +"\\|wb\\|.*")){
					for(KeyValue kv : rs.raw()){
						if("content".equals(new String(kv.getQualifier()))){
							tweets.add(new String( kv.getValue() ) );
						}
					}
					if(tweets.size()==100)
						break;
				}
			}
			scanner.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static Options initOptions() {
		Options options = new Options();
		options.addOption("i","id",true,"User ID to be analyzed.");
		options.addOption("j","justifiedcount",true,"Justified count for determing Top K.");
		options.addOption("o","oneway",false,"Whether only to use oneway follow infomation");
		options.addOption("occ","occurence",true,"Filter tag occurence");
		options.addOption("m","min",true,"Minimum fans threshold,default is 0");
		options.addOption("x","max",true,"Maximum fans threshold,default is 500000");	
		return options;
	}

	public static boolean validateOptions(CommandLine cmdLine) {
		if( !cmdLine.hasOption("i") ) return false;

		if( cmdLine.hasOption("m") && !StringUtil.isNumeric(cmdLine.getOptionValue("m")) )
			return false;
		else if( cmdLine.hasOption("x") && !StringUtil.isNumeric(cmdLine.getOptionValue("x")) )
			return false;
		else if( cmdLine.hasOption("j") && !StringUtil.isNumeric(cmdLine.getOptionValue("j")) )
			return false;
		else if( cmdLine.hasOption("occ") && !StringUtil.isNumeric(cmdLine.getOptionValue("occ")) )
			return false;
		else
			return true;
	}

	public static void printUsage(Options options){
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("analyzSinaUserPref -i <uid> [-j <justified_count>] [-occ <occurence_count>] [-oneway] [-m <minfans>] [-x <maxfans>]", options);
	}

	public static void main(String[] args) throws IOException {
		CommandLineParser parser = new BasicParser();
		CommandLine cmdLine = null;
		Options options = initOptions();
		try {
			cmdLine = parser.parse(options, args);
		} catch (ParseException e) {
			printUsage(options);
			System.exit(1);
		}
		if(!validateOptions(cmdLine)){
			printUsage(options);
			System.exit(1);
		}		

		String uid = cmdLine.getOptionValue("id");
		int minFans = 0;
		if(cmdLine.hasOption("min"))
			minFans = Integer.parseInt( cmdLine.getOptionValue("min") );

		int maxFans = 500000;
		if(cmdLine.hasOption("max"))
			maxFans = Integer.parseInt( cmdLine.getOptionValue("max") );

		int justifiedCount = 2;
		if(cmdLine.hasOption("j"))
			justifiedCount = Integer.parseInt( cmdLine.getOptionValue("j") );
		
		int occurenceCount = 1;
		if(cmdLine.hasOption("occ"))
			occurenceCount = Integer.parseInt( cmdLine.getOptionValue("occ") );
		
		boolean oneway = false;
		if(cmdLine.hasOption("o"))
			oneway = true;

		AnalyzSinaUserPref tagger = new AnalyzSinaUserPref(occurenceCount,justifiedCount,oneway,minFans,maxFans);
		Map<String,Integer> tagOccurences = new HashMap<String,Integer>();
		Map<String,Map<String,Double>> tagFollows = new HashMap<String,Map<String,Double>>();
		List<WeightedTag> wts = tagger.analyzUser(uid,tagOccurences,tagFollows);
		System.out.println("#Results(" + wts.size() + "):");
		System.out.println("Occurence\tWeight\tTag\tFollow");
		for(WeightedTag wt : wts)
			System.out.println(tagOccurences.get(wt.getTag()) + "\t" + wt.toString() + "\t"
					+ getTopWeightedTagFollow(tagFollows,wt.getTag(),3));
	}
	
	public static String getTopWeightedTagFollow(Map<String,Map<String,Double>> follows,String tag,int topk){
		StringBuilder sb = new StringBuilder();
		List<Entry<String,Double>> list = new ArrayList<Entry<String,Double>>(follows.get(tag).entrySet());
		Collections.sort(list,new EntryValueComparator());
		if(topk > list.size())
			topk = list.size();
		for(int i=0;i<topk;++i){
			Entry<String,Double> e = list.get(i);
			sb.append(e.getKey());
			sb.append(':');
			sb.append(String.format("%.5s", e.getValue()));
			sb.append(',');
		}
		if(sb.length()>0)
			sb.deleteCharAt( sb.length()-1 );
		return sb.toString();
	}
}
