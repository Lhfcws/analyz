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
 * 基于用户的单向关注信息，预测用户的兴趣标签。
 * 设计逻辑：
 * 1. 获取自身以及一定粉丝数范围内的单向关注用户在AnalyzUserTagMR输出的结果tag_dist
 * 2. 基于每个tag_dist，计算每个标签对用户兴趣的贡献值。
 *    confidence = tweets_covered_count / tweets_total_count
 *    p(tag,user) = confidence * (alpha + tag_occurences) / (n * alpha + total_tag_occurences)， 其中n表示tag_dist中有多少个标签
 * 3. 累加自己以及所有用户的所有标签权值
 * 4. 输出结果
 * 
 * 输出位置：yeezhao.user.profile:analyz:meta_group
 * 保存值的格式：cs$ca|g1$occ1$w1|g2$occ2$w2|...  （列表按w降序排序）
 * cs=0表示没有累加用户自身标签，cs=1表示累加了用户自身标签
 * ca表示总共累加的有效的单向关注用户标签的数目，不含自身标签
 * g：表示某些meta label，与MR2结合的话，就是tag
 * occ：表示某个meta label出现的频数
 * w：表示某个meta label相对于用户的权值（保留3位小数点,权值＜0.01的标签全部过滤掉。）
 * 
 * 参数说明: 
 * alpha - 平滑函数，用以平滑那些tag在用户微博中没有出现的情况
 * flw_rng - 单向关注的粉丝数范围
 * min_occ - 最少出现的tag次数
 * 
 * @author yingbin/arber
 *
 */
public class MetaGroupLabellerMR implements CliRunner {
	private static final String CMD_NAME = "metaGroupLabeller";
	private static final String CLI_ALPHA = "alpha";
	private static final String CLI_FLW_RNG = "frange";
	private static final String CLI_MIN_OCC = "occ";
	private static final String CLI_THRESHOLD = "threshold";

	private static final String TAG_DIST_COUNT_KEY = "tag_dist.#"; 

	private static final String CONF_REDIS_HOST = "redis.host";
	private static final String CONF_REDIS_PORT = "redis.port";

	private static final Log LOG = LogFactory.getLog(MetaGroupLabellerMR.class);

	//Default parameters
	private static final int DEFAULT_ALPHA = 1; //Smoothing factor
	private static final String DEFAULT_FLW_RNG = "1000,500000"; //followee range limit
	private static final int DEFALUT_MIN_OCC = 1; //min tag occurrence
	private static final String DEFAULT_REDIS_HOST = "localhost";
	private static final int DEFAULT_REDIS_PORT = 6379;
	private static final double DEFAULT_THRESHOLD = 0.01;

	private static String getRedisAnalyzTagDistKey(String rowkey){
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
			jedis.set(getRedisAnalyzTagDistKey( Bytes.toString(row.get())), tagDist);
			jedis.incr(TAG_DIST_COUNT_KEY);
		}
	}	

	//Input columns
	private static class AnalyzMapper extends TableMapper<ImmutableBytesWritable,Put> {

		enum COUNTER{TOTAL, SUCCESS, EMPTY, USER_OUTLIER, FRIEND_OUTLIER, 
			USER_MISS_FRIEND, USER_MISS_INFO, FRIEND_MISS_INFO,TAG_DIST}

		private int alpha = DEFAULT_ALPHA;
		private int minOcc = DEFALUT_MIN_OCC;
		private double threshold = 0.01;
		private int followeeMinFans;
		private int followeeMaxFans;
		private Jedis jedis = null;

		@Override
		public void setup(Context context) throws IOException{
			Configuration conf = context.getConfiguration();
			alpha = conf.getInt(CLI_ALPHA, DEFAULT_ALPHA);
			minOcc = conf.getInt(CLI_MIN_OCC, DEFALUT_MIN_OCC);
			threshold = conf.getFloat(CLI_THRESHOLD, (float)DEFAULT_THRESHOLD);
			String redisHost = conf.get(CONF_REDIS_HOST,DEFAULT_REDIS_HOST);
			int redisPort = conf.getInt(CONF_REDIS_PORT,DEFAULT_REDIS_PORT);
			jedis = new Jedis(redisHost, redisPort);
			System.out.println(String.format("Parameter setting: alpha=%d, minOcc=%d, " +
					"followeeMinFans=%d, followeeMaxFans=%d, threshold=%f", alpha, minOcc, followeeMinFans, followeeMaxFans, threshold));			
		}

		FreqDist<String>  tagOccurences = new FreqDist<String>();
		Map<String,Double> tagWeights = new HashMap<String,Double>();						

		public void map(ImmutableBytesWritable row, Result rs, Context context)
				throws IOException, InterruptedException {
			context.getCounter(COUNTER.TOTAL).increment(1);			
			tagOccurences.clear();
			tagWeights.clear();
			String rowkey = Bytes.toString(row.get());
			String src = AppUtil.getUsrSrc(rowkey);

			int accSelf = 0;
			//用户自己的标签也加入计算
			String tagDist = HBaseUtil.getColumnValue(rs, AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_TAG_DIST);
			if(tagDist != null){
				addUserTagDist(tagDist);
				accSelf = 1;
			}
			//累加所有单向用户的标签权重
			List<String> fanUids = new ArrayList<String>();
			ResultParser.fillFriends(rs, fanUids, false);
			if( fanUids.size() == 0 ){
				context.getCounter(COUNTER.USER_MISS_FRIEND).increment(1);
				return;
			}

			int accumulated = 0;

			for( String fanUid : fanUids ){
				String fanRowkey = src + StringUtil.DELIMIT_1ST + fanUid;
				tagDist = jedis.get(getRedisAnalyzTagDistKey(fanRowkey));
				if(tagDist == null)
					continue;
				context.getCounter(COUNTER.TAG_DIST).increment(1);
				addUserTagDist(tagDist);				
				accumulated++;
			}
			if(accSelf == 0 && accumulated == 0){
				context.getCounter(COUNTER.EMPTY).increment(1);
				return;
			}
			context.getCounter(COUNTER.SUCCESS).increment(1);
			StringBuilder sb = new StringBuilder();
			sb.append(accSelf).append(StringUtil.DELIMIT_2ND).append(accumulated);
			List<Entry<String,Double>> list = new ArrayList<Entry<String,Double>>(tagWeights.entrySet());
			Collections.sort(list,new AnalyzSinaUserPref.EntryValueComparator());
			for(Entry<String,Double> e : list){
				if( tagOccurences.getCount(e.getKey()) >= minOcc && e.getValue() >= threshold ){
					sb.append(StringUtil.DELIMIT_1ST).append(e.getKey())
					.append(StringUtil.DELIMIT_2ND)
					.append(tagOccurences.get(e.getKey()).toString())
					.append(StringUtil.DELIMIT_2ND)
					.append(String.format("%.3f", e.getValue()));
				}
			}
			Put put = new Put(row.get());
			put.add(AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_META_GROUP, Bytes.toBytes(sb.toString()));			
			context.write(row, put);
		}

		/**
		 * 基于tag_dist计算用户标签权重
		 * 
		 * @param tagDist
		 */
		public void addUserTagDist(String tagDist){
			if(tagDist == null || tagDist.length()==0 )
				return;
			String[] dists = tagDist.split("\\|");
			String[] tweetsCount = dists[0].split("\\$");
			double confidence = Double.parseDouble(tweetsCount[0])/Double.parseDouble(tweetsCount[1]);
			int sum = alpha * (dists.length-1);
			for(int i=1;i<dists.length;++i){
				String[] tagWeight = dists[i].split("\\$");
				int count = Integer.parseInt(tagWeight[1]);
				sum += count;
				tagOccurences.incr(tagWeight[0]);
			}
			for(int i=1;i<dists.length;++i){
				String[] tagWeight = dists[i].split("\\$");
				int count = Integer.parseInt(tagWeight[1]);
				double weight = confidence * (alpha + count) / sum;
				Double tw = tagWeights.get(tagWeight[0]);
				if(tw == null){
					tw = weight;
				}else{
					tw += weight;
				}
				tagWeights.put(tagWeight[0], tw);
			}
		}
	}

	@SuppressWarnings("static-access")
	@Override
	public Options initOptions() {
		Options options = new Options();
		options.addOption(OptionBuilder.create(AdvCli.CLI_PARAM_HELP));
		options.addOption( AdvCli.CLI_PARAM_ALL, false, "label all users");
		options.addOption(OptionBuilder.withArgName("rowkey").hasArg().withDescription("start rowkey").create(AdvCli.CLI_PARAM_S));
		options.addOption(OptionBuilder.withArgName("rowkey").hasArg().withDescription("end rowkey").create(AdvCli.CLI_PARAM_E));
		options.addOption(OptionBuilder.withArgName("min,max").hasArg().withDescription("take into account followees with fans in [min,max]; default:1000,500000").create(CLI_FLW_RNG));
		options.addOption(OptionBuilder.withArgName("int").hasArg().withDescription("min tag occurrence; default:1").create(CLI_MIN_OCC));
		options.addOption(OptionBuilder.withArgName("int").hasArg().withDescription("smoothing factor for calculating tag weight; default:1").create(CLI_ALPHA));
		options.addOption(OptionBuilder.withArgName("float").hasArg().withDescription("threshold value for meta_group label; default:0.01").create(CLI_THRESHOLD));
		return options;
	}

	@Override
	public void start(CommandLine cmd) {
		Configuration conf = AnalyzConfiguration.getInstance();
		try {
			String jobName = AdvCli.genStandardJobName(CMD_NAME, cmd);
			System.out.println("Start run " + jobName + "...");
			
			String redisHost = conf.get(CONF_REDIS_HOST,DEFAULT_REDIS_HOST);
			int redisPort = conf.getInt(CONF_REDIS_PORT,DEFAULT_REDIS_PORT);
			
			conf.set(CLI_FLW_RNG, cmd.getOptionValue(CLI_FLW_RNG, DEFAULT_FLW_RNG));
			conf.setInt(CLI_MIN_OCC, Integer.parseInt(cmd.getOptionValue(CLI_MIN_OCC, Integer.toString(DEFALUT_MIN_OCC))));
			conf.setInt(CLI_ALPHA, Integer.parseInt(cmd.getOptionValue(CLI_ALPHA, Integer.toString(DEFAULT_ALPHA))));
			conf.setFloat(CLI_THRESHOLD, Integer.parseInt(cmd.getOptionValue(CLI_THRESHOLD, Double.toString(DEFAULT_THRESHOLD))));
			
			Jedis jedis = new Jedis(redisHost, redisPort);
			String tagDistCount = jedis.get(TAG_DIST_COUNT_KEY);
			jedis.disconnect();
			
			if(tagDistCount != null && Integer.parseInt(tagDistCount)>0){
				System.out.println("tag_dist has been cached.");
			}else{
				//Step1: Cache tag_dist
				Job job = new Job(conf, String.format("cache tag_dist to Redis@%s:%d",redisHost, redisPort));		
				job.setJarByClass(MetaGroupLabellerMR.class);
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

				System.out.print("caching tag_dist ... ");
				job.waitForCompletion(true);
				if(!job.isSuccessful()){
					System.out.println("[failed]");
					System.exit(1);
				}
				
				jedis = new Jedis(redisHost, redisPort);
				tagDistCount = jedis.get(TAG_DIST_COUNT_KEY);
				if(tagDistCount == null){
					System.out.println("[failed]");
					System.exit(1);
				}else{
					System.out.println("[ok] " + tagDistCount + "cached.");
				}
				jedis.disconnect();
			}
			
			//Calculate meta_group
			Job job = new Job(conf, jobName);		
			job.setJarByClass(MetaGroupLabellerMR.class);
			job.setNumReduceTasks(0);
			
			Scan scan = new Scan();
			HBaseUtil.setScanRange(scan, cmd);
			scan.addColumn(AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_TAG_DIST);
			scan.addFamily(AppConsts.COL_FMLY_FRIEND);
			scan.setCaching(500);
			scan.setCacheBlocks(false);

			TableMapReduceUtil.initTableMapperJob(
					conf.get(AppConsts.HTBL_USER_PROFILE), scan, AnalyzMapper.class,
					ImmutableBytesWritable.class, Put.class, job);
			TableMapReduceUtil.initTableReducerJob(conf.get(AppConsts.HTBL_USER_PROFILE), null, job);

			System.out.println("calculating meta_group ... ");
			job.waitForCompletion(true);

		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("", e);
		}
	}

	@Override
	public boolean validateOptions(CommandLine cmd) {
		boolean res = cmd.hasOption(AdvCli.CLI_PARAM_ALL) || (cmd.hasOption(AdvCli.CLI_PARAM_S) && cmd.hasOption(AdvCli.CLI_PARAM_E));
		if(res){
			try{
				if(cmd.hasOption(CLI_ALPHA))
					Integer.parseInt(cmd.getOptionValue(CLI_ALPHA));
				if(cmd.hasOption(CLI_FLW_RNG)){
					String[] ranges = cmd.getOptionValue(CLI_FLW_RNG).split(",");
					int rm = Integer.parseInt(ranges[0]);
					int rx = Integer.parseInt(ranges[1]);
					if(rm <0 || rm > rx)
						return false;
				}
				if(cmd.hasOption(CLI_THRESHOLD)){
					Double.parseDouble(cmd.getOptionValue(CLI_THRESHOLD));
				}
				return true;
			} catch (Exception e){
				return false;
			}
		}
		return false;
	}

	public static void main(String[] args) {
		AdvCli.initRunner(args, "metaGroupLabeller [-s [startRow] -e [endRow]|-all] [-alpha <int>] [-frange <min,max>] [-occ <int>] [-t <float>]", new MetaGroupLabellerMR());
	}
}
