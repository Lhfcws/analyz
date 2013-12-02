package com.yeezhao.analyz.group;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.Job;

import com.yeezhao.amkt.dujob.DUConsts;
import com.yeezhao.analyz.util.AnalyzConfiguration;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.analyz.util.AppUtil;
import com.yeezhao.analyz.util.ResultParser;
import com.yeezhao.analyz.util.UserProfileExporter;
import com.yeezhao.commons.hbase.HBaseUtil;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;
import com.yeezhao.commons.util.StringUtil;

/**
 * 计算用户标签权重。
 * 
 * 输出关联的位置：yeezhao.user.profile:analyz:tag_dist
 * 保存值的格式：tweets_covered_count$tweets_total_count|t1$c1|t2$c2|...
 * tweets_covered_count表示用户的所有标签覆盖到的微博条数 tweets_total_count表示分析用户所用的所有微博总条数
 * t：表示用户的某个tag（如果tag中有用户昵称的去除此tag） c：表示包含某个tag的微博条数
 * 
 * @author yingbin
 * 
 */

public class AnalyzUserTagMR implements CliRunner {

	private static final String CLI_SRC = "src";
	private static final String CLI_RNG = "range";
	private static final String CLI_ALL = "all";
	private static final byte[] COL_FMLY_CRAWL = Bytes.toBytes("crawl");
	private static final String CONF_SRC = "user.src";
	private static final String CONF_RNG = "user.fans.range";
	private static final String CONF_ALL = "user.fans.all";
	private static final byte[] COL_FMLY_ANALYZ = Bytes.toBytes("analyz");
	private static final byte[] COL_USER_TYPE = Bytes.toBytes("user_type");
	private static final Log LOG = LogFactory.getLog(AnalyzUserTagMR.class);

	static class AnalyzMapper extends TableMapper<ImmutableBytesWritable, Put> {

		enum COUNTER {
			TOTAL, SUCCESS, OUTLIER, MISS_INFO, MISS_TAG, MISS_WB, NORMAL_USER, ORG_USER, ROBOT_USER, UNKNOWN_USER
		}

		private String src = null;
		private boolean all = false;
		private int minFans = -1;
		private int maxFans = -1;
		private UserProfileExporter export = new UserProfileExporter();

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			export.setup(conf, conf.get(AppConsts.HTBL_USER_PROFILE));
			src = conf.get(CONF_SRC);
			all = conf.getBoolean(CONF_ALL, false);
			if (all)
				return;
			String[] ranges = conf.get(CONF_RNG, "1000,500000").split(",");
			minFans = Integer.parseInt(ranges[0]);
			maxFans = Integer.parseInt(ranges[1]);
		}

		Map<String, String> infos = new HashMap<String, String>();
		Map<String, Integer> tagsCounter = new HashMap<String, Integer>();
		List<String> tags = new ArrayList<String>();
		List<String> tweets = new ArrayList<String>();
		private final byte[] FAMILY_ANALYZ_BYTES = Bytes.toBytes("analyz");
		private final byte[] QUALIFIER_TAG_DIST_BYTES = Bytes
				.toBytes("tag_dist");

		public void map(ImmutableBytesWritable rowkey, Result row,
				Context context) throws IOException, InterruptedException {
			if (!AppUtil.validateUser(
					row.getValue(COL_FMLY_ANALYZ, COL_USER_TYPE),
					context.getCounter(COUNTER.NORMAL_USER),
					context.getCounter(COUNTER.ORG_USER),
					context.getCounter(COUNTER.ROBOT_USER),
					context.getCounter(COUNTER.UNKNOWN_USER))) {
				return;
			}

			String uid = ResultParser.getUid(Bytes.toString(rowkey.get()));
			if (uid == null) {
				return;
			}
			infos.clear();
			ResultParser.fillInfos(row, infos);
			context.getCounter(COUNTER.TOTAL).increment(1);

			if (!infos.containsKey("followers_count")
					|| !infos.containsKey("name")) {
				context.getCounter(COUNTER.MISS_INFO).increment(1);
				return;
			}
			int fans = Integer.parseInt(infos.get("followers_count"));
			if (!all && (fans < minFans || fans > maxFans)) {
				context.getCounter(COUNTER.OUTLIER).increment(1);
				return;
			}

			tags.clear();
			tagsCounter.clear();
			tweets.clear();

			String rowPrefix = src + StringUtil.DELIMIT_1ST + uid
					+ StringUtil.DELIMIT_1ST;
			// 获取标签和微博
			export.getTagsNTweets(rowPrefix, tags, tweets);
			for (String tag : tags)
				tagsCounter.put(tag.toLowerCase(), 0);

			// 删除昵称标签
			tagsCounter.remove(infos.get("name").toLowerCase());
			if (tagsCounter.size() == 0) {
				context.getCounter(COUNTER.MISS_TAG).increment(1);
				return;
			} else if (tweets.size() == 0) {
				context.getCounter(COUNTER.MISS_WB).increment(1);
				return;
			}
			// 将大写字母转为小写字母
			for (int i = 0; i < tweets.size(); ++i)
				tweets.set(i, tweets.get(i).toLowerCase());

			context.getCounter(COUNTER.SUCCESS).increment(1);
			int coveredCount = 0;
			// 统计标签的出现的微博数目，以及所有标签覆盖到的微博数目
			for (String tweet : tweets) {
				boolean covered = false;
				for (Entry<String, Integer> e : tagsCounter.entrySet())
					if (tweet.indexOf(e.getKey()) != -1) {
						tagsCounter.put(e.getKey(), 1 + e.getValue());
						covered = true;
					}
				if (covered)
					coveredCount++;
			}
			// 输出结果
			StringBuilder sb = new StringBuilder();
			sb.append(coveredCount);
			sb.append(StringUtil.DELIMIT_2ND);
			sb.append(tweets.size());
			for (Entry<String, Integer> e : tagsCounter.entrySet()) {
				sb.append(StringUtil.DELIMIT_1ST);
				sb.append(e.getKey());
				sb.append(StringUtil.DELIMIT_2ND);
				sb.append(e.getValue().toString());
			}
			byte[] value = Bytes.toBytes(sb.toString());
			Put put = new Put(rowkey.get());
			put.add(FAMILY_ANALYZ_BYTES, QUALIFIER_TAG_DIST_BYTES, value);
			context.write(rowkey, put);
		}

		@Override
		public void cleanup(Context context) throws IOException {
			export.cleanup();
		}
	}

	private boolean startJob(Configuration conf, DUConsts.DATA_SRC src,
			boolean all, String range) throws IOException,
			InterruptedException, ClassNotFoundException {
		String jobName = String.format("analyz tag for %s", src);

		conf.set(CONF_SRC, src.toString());
		conf.setBoolean(CONF_ALL, all);
		conf.set(CONF_RNG, range);

		Job job = new Job(conf, jobName);
		job.setJarByClass(AnalyzUserTagMR.class);
		job.setNumReduceTasks(0);

		String rowPrefix = src.toString() + StringUtil.DELIMIT_1ST;
		Scan scan = new Scan();
		scan.addFamily(COL_FMLY_CRAWL);
		scan.addColumn(COL_FMLY_ANALYZ, COL_USER_TYPE);
		scan.setStartRow(Bytes.toBytes(rowPrefix));
		scan.setStopRow(Bytes.toBytes(HBaseUtil.getPrefixUpperBound(rowPrefix)));
//		scan.setStopRow(Bytes.toBytes("sn|1078625370"));	//first one million user
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs

		TableMapReduceUtil.initTableMapperJob(
				conf.get(AppConsts.HTBL_USER_PROFILE), scan,
				AnalyzMapper.class, ImmutableBytesWritable.class, Put.class,
				job);
		TableMapReduceUtil.initTableReducerJob(
				conf.get(AppConsts.HTBL_USER_PROFILE), null, job);

		return job.waitForCompletion(true);
	}

	@SuppressWarnings("static-access")
	@Override
	public Options initOptions() {
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("type").hasArg()
				.withDescription("sn->sina,tx->tencent").create(CLI_SRC));
		options.addOption(OptionBuilder.withDescription("analyze all user")
				.create(CLI_ALL));
		options.addOption(OptionBuilder.withArgName("min,max").hasArg()
				.withDescription("analyze users with fans in [min,max]")
				.create(CLI_RNG));
		return options;
	}

	@Override
	public void start(CommandLine cmd) {
		Configuration conf = AnalyzConfiguration.getInstance();
		try {
			startJob(conf,
					DUConsts.DATA_SRC.valueOf(cmd.getOptionValue(CLI_SRC)),
					cmd.hasOption(CLI_ALL), cmd.getOptionValue(CLI_RNG, "0,0"));
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("", e);
		}
	}

	@Override
	public boolean validateOptions(CommandLine cmd) {
		boolean res = cmd.hasOption(CLI_SRC);
		res = res && (cmd.hasOption(CLI_ALL) || cmd.hasOption(CLI_RNG));
		if (res) {
			try {
				DUConsts.DATA_SRC.valueOf(cmd.getOptionValue(CLI_SRC));
				if (cmd.hasOption(CLI_RNG)) {
					String[] ranges = cmd.getOptionValue(CLI_RNG).split(",");
					int min = Integer.parseInt(ranges[0]);
					int max = Integer.parseInt(ranges[1]);
					if (min >= 0 && max >= min)
						return true;
					else
						return false;
				} else
					return true;
			} catch (Exception e) {
				return false;
			}
		}
		return false;
	}

	public static void main(String[] args) {
		AdvCli.initRunner(args,
				"analyzUserTagMR -src <sn|tx> -all|-range <min,max>",
				new AnalyzUserTagMR());
	}
}
