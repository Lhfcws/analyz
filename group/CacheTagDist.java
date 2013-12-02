package com.yeezhao.analyz.group;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import redis.clients.jedis.Jedis;

import com.yeezhao.analyz.util.AnalyzConfiguration;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.analyz.util.AppUtil;
import com.yeezhao.analyz.util.ResultParser;
import com.yeezhao.commons.hbase.HBaseUtil;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;
import com.yeezhao.commons.util.FreqDist;
import com.yeezhao.commons.util.StringUtil;

/**
 * 
 * @author yingbin
 *
 */
public class CacheTagDist implements CliRunner {
	private static final String CMD_NAME = "cacheTagDist";	
	private static final String CLI_FLW_RNG = "frange";
	private static final String CLI_DO_CACHE = "cache";
	private static final String CLI_DO_CLEAN = "clean";
	
	private static final String TAG_DIST_COUNT_KEY = "tag_dist.#"; 
	
	private static final String CONF_REDIS_HOST = "redis.host";
	private static final String CONF_REDIS_PORT = "redis.port";
	
	private static final Log LOG = LogFactory.getLog(CacheTagDist.class);
	
	//Default parameters
	private static final String DEFAULT_FLW_RNG = "1000,500000"; //followee range limit
	private static final String DEFAULT_REDIS_HOST = "localhost";
	private static final int DEFAULT_REDIS_PORT = 6379;
	
	private static String getRedisTagDistKey(String rowkey){
		return "tag_dist." + rowkey;
	}
	private static class CacheTagDistMapper extends TableMapper<NullWritable,NullWritable> {
		
		private int followeeMinFans = 1000;
		private int followeeMaxFans = 500000;
		Jedis jedis = null;
		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			String redisHost = conf.get(CONF_REDIS_HOST,DEFAULT_REDIS_HOST);
			int redisPort = conf.getInt(CONF_REDIS_PORT,DEFAULT_REDIS_PORT);
			jedis = new Jedis(redisHost, redisPort);
			String[] ranges = conf.get(CLI_FLW_RNG).split(",");
			followeeMinFans = Integer.parseInt(ranges[0]);
			followeeMaxFans = Integer.parseInt(ranges[1]);
		}
		
		enum COUNTER{TOTAL, SUCCESS};
		
		@Override
		public void map(ImmutableBytesWritable row, Result rs, Context context) throws IOException, InterruptedException {
			context.getCounter(COUNTER.TOTAL).increment(1);
			String tagDist = AppUtil.getColumnValue(rs, AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_TAG_DIST);
			if(tagDist == null || tagDist.length()==0)
				return;
			String followerCount = AppUtil.getColumnValue(rs, AppConsts.COL_FMLY_CRAWL, AppConsts.COL_FOLLOWCOUNT);
			if(followerCount == null || followerCount.length()==0)
				return;
			int count = Integer.parseInt(followerCount);
			if(count < followeeMinFans || count > followeeMaxFans)
				return;
			context.getCounter(COUNTER.SUCCESS).increment(1);
			jedis.set(getRedisTagDistKey( Bytes.toString(row.get())), tagDist);
			jedis.incr(TAG_DIST_COUNT_KEY);
		}
	}
	
	private static void cleanTagDistCache(String host,int port){
		Jedis jedis = new Jedis(host,port);
		int count = 0;
		for(String key : jedis.keys("tag_dist.*")){
			jedis.del(key);
			count += 1;
		}
		System.out.println("[ok] " + count + " keys deleted.");
		jedis.disconnect();
	}
	
	

	@SuppressWarnings("static-access")
	@Override
	public Options initOptions() {
		Options options = new Options();
		options.addOption(OptionBuilder.create(AdvCli.CLI_PARAM_HELP));
		options.addOption(OptionBuilder.withArgName("min,max").hasArg().withDescription("take into account followees with fans in [min,max]; default:1000,500000").create(CLI_FLW_RNG));
		options.addOption(OptionBuilder.withDescription("do cache").create(CLI_DO_CACHE));
		options.addOption(OptionBuilder.withDescription("do clean").create(CLI_DO_CLEAN));
		return options;
	}

	@Override
	public void start(CommandLine cmd) {
		Configuration conf = AnalyzConfiguration.getInstance();
		String redisHost = conf.get(CONF_REDIS_HOST,DEFAULT_REDIS_HOST);
		int redisPort = conf.getInt(CONF_REDIS_PORT,DEFAULT_REDIS_PORT);
		
		if(cmd.hasOption(CLI_DO_CLEAN)){			
			cleanTagDistCache(redisHost, redisPort);
			return;
		}
		
		try {
			String jobName = AdvCli.genStandardJobName(CMD_NAME, cmd);
			System.out.println("Start run " + jobName + "...");
						
			conf.set(CLI_FLW_RNG, cmd.getOptionValue(CLI_FLW_RNG, DEFAULT_FLW_RNG));
			
			Job job = new Job(conf, String.format("cache tag_dist to Redis@%s:%d",redisHost, redisPort));		
			job.setJarByClass(CacheTagDist.class);
			job.setNumReduceTasks(0);
			
			Scan scan = new Scan();
			scan.addColumn(AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_TAG_DIST);
			scan.addColumn(AppConsts.COL_FMLY_CRAWL, AppConsts.COL_FOLLOWCOUNT);
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			
			TableMapReduceUtil.initTableMapperJob(
					conf.get(AppConsts.HTBL_USER_PROFILE), scan, CacheTagDistMapper.class,
					NullWritable.class, NullWritable.class, job);
			TableMapReduceUtil.initTableReducerJob(conf.get(AppConsts.HTBL_USER_PROFILE), null, job);
			
			System.out.print("start caching tag_dist ... ");
			job.waitForCompletion(true);
			if(!job.isSuccessful()){
				System.out.println("[failed]");
				System.exit(1);
			}
			
			Jedis jedis = new Jedis(redisHost, redisPort);
			String tagDistCount = jedis.get(TAG_DIST_COUNT_KEY);
			if(tagDistCount == null){
				System.out.println("[failed]");
				System.exit(1);
			}else{
				System.out.println("[ok] " + tagDistCount + " items cached.");
			}
			jedis.disconnect();			
			
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("", e);
		}
	}

	@Override
	public boolean validateOptions(CommandLine cmd) {
		boolean res = cmd.hasOption(CLI_DO_CACHE) || cmd.hasOption(CLI_DO_CLEAN);
		if(res){
			try{
				if(cmd.hasOption(CLI_FLW_RNG)){
					String[] ranges = cmd.getOptionValue(CLI_FLW_RNG).split(",");
					int rm = Integer.parseInt(ranges[0]);
					int rx = Integer.parseInt(ranges[1]);
					if(rm <0 || rm > rx)
						return false;
				}
				return true;
			} catch (Exception e){
				return false;
			}
		}
		return false;
	}

	public static void main(String[] args) {
		AdvCli.initRunner(args, "cacheTagDist -cache|-clean [-frange <min,max>]", new CacheTagDist());
	}
}
