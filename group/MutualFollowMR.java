package com.yeezhao.analyz.group;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.yeezhao.amkt.dujob.DUConsts.DATA_SRC;
import com.yeezhao.analyz.util.AnalyzConfiguration;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.analyz.util.AppUtil;
import com.yeezhao.analyz.util.ResultParser;
import com.yeezhao.commons.hbase.HBaseUtil;
import com.yeezhao.commons.mapred.CountReducer;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;
import com.yeezhao.commons.util.StringUtil;

/**
 * 分析粉丝的互粉关系
 * 算法: 
 * 		Map: 对每一个单向关注，输出一个uid pair, 按字母序排序
 * 		Reduce: 如果某一个pair出现一次以上，则其为双向关注关系
 * 输出位置：yeezhao.user.profile:friend:<uid>
 * 只设置是双向关注的记录，格式为：friend:<uid>=1，其它情况则保留空值。
 * 
 * @author hans & yingbin
 */

public class MutualFollowMR implements CliRunner {	
	private static final String CLI_SRC = "src";
	private static final byte[] COL_FMLY_ANALYZ = Bytes.toBytes("analyz");
	private static final byte[] COL_USER_TYPE = Bytes.toBytes("user_type");
	private static final String CONF_SRC = "user.src";
	private static final Log LOG = LogFactory.getLog(MutualFollowMR.class);

	static class MutualMapper extends TableMapper<Text,IntWritable> {

		enum COUNTER{TOTAL, SUCCESS, FAIL, NORMAL_USER, ORG_USER, ROBOT_USER, UNKNOWN_USER}

		private Text text = new Text();
		private IntWritable one = new IntWritable(1);
		List<String> friends = new ArrayList<String>();


		public void map(ImmutableBytesWritable rowkey, Result row, Context context) throws IOException, InterruptedException {
			if(!AppUtil.validateUser(row.getValue(COL_FMLY_ANALYZ, COL_USER_TYPE), context.getCounter(COUNTER.NORMAL_USER), context.getCounter(COUNTER.ORG_USER), 
					context.getCounter(COUNTER.ROBOT_USER), context.getCounter(COUNTER.UNKNOWN_USER))){
				return;
			}
			friends.clear();
			ResultParser.fillFriends(row, friends);
			context.getCounter(COUNTER.TOTAL).increment(1);
			if(friends.isEmpty()){
				context.getCounter(COUNTER.FAIL).increment(1);
			} else {
				context.getCounter(COUNTER.SUCCESS).increment(1);
				String user = Bytes.toString(rowkey.get()).split(StringUtil.STR_DELIMIT_1ST)[1];
				for(String uid : friends){
					if( user.compareTo(uid) < 0){
						text.set(user + "," + uid);
					}else{
						text.set(uid + "," + user);
					}
					context.write(text, one);
				}
			}
		}
	}
	static class MutualReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable>  {
		enum COUNTER{MUTUAL, ONEWAY}
		private String SRC = null;

		@Override
		public void setup(Context context){			
			SRC = context.getConfiguration().get(CONF_SRC);
		}

		public byte[] getRowkey(String id){
			return Bytes.toBytes(SRC + StringUtil.DELIMIT_1ST + id);
		}

		byte[] ONE = Bytes.toBytes("1");

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values)
				count += val.get();					

			if(count == 2){
				context.getCounter(COUNTER.MUTUAL).increment(1);
				String[] pair = key.toString().split(",");
				Put put = new Put(getRowkey(pair[0]));
				put.add(AppConsts.COL_FMLY_FRIEND, Bytes.toBytes(pair[1]), ONE);
				context.write(null, put);
				put = new Put(getRowkey(pair[1]));
				put.add(AppConsts.COL_FMLY_FRIEND, Bytes.toBytes(pair[0]), ONE);
				context.write(null, put);
			}else
				context.getCounter(COUNTER.ONEWAY).increment(1);
		}
	}

	private boolean startJob(Configuration conf, DATA_SRC src) throws IOException, InterruptedException, ClassNotFoundException{
		String jobName = String.format("analyz mutual fans for %s", src);
		System.out.println(jobName + "...");
		
		conf.set(CONF_SRC, src.toString());

		Job job = new Job(conf, jobName);
		job.setJarByClass(MutualFollowMR.class);
		job.setNumReduceTasks(30);

		String rowPrefix = src.toString() + StringUtil.DELIMIT_1ST;
		Scan scan = new Scan();
		scan.addFamily(AppConsts.COL_FMLY_FRIEND);
		scan.addColumn(COL_FMLY_ANALYZ, COL_USER_TYPE);
		scan.setStartRow(Bytes.toBytes(rowPrefix));
		scan.setStopRow(Bytes.toBytes(HBaseUtil.getPrefixUpperBound(rowPrefix)));
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs

		TableMapReduceUtil.initTableMapperJob(
				conf.get(AppConsts.HTBL_USER_PROFILE), scan, MutualMapper.class,
				Text.class, IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(
				conf.get(AppConsts.HTBL_USER_PROFILE),
				MutualReducer.class,
				job);
		job.setCombinerClass(CountReducer.class);

		return job.waitForCompletion(true);
	}

	@SuppressWarnings("static-access")
	@Override
	public Options initOptions() {
		Options options = new Options();
		options.addOption( OptionBuilder.withArgName("user type")
				.hasArg().withDescription("sn->sina,tx->tencent").create(CLI_SRC));
		return options;
	}

	@Override
	public void start(CommandLine cmd) {
		Configuration conf = AnalyzConfiguration.getInstance();
		try {
			startJob(conf, DATA_SRC.valueOf(cmd.getOptionValue(CLI_SRC)));
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("", e);
		} 
	}

	@Override
	public boolean validateOptions(CommandLine cmd) {
		if(cmd.hasOption(CLI_SRC)){
			try{
				DATA_SRC.valueOf(cmd.getOptionValue(CLI_SRC));
				return true;
			} catch (Exception e){
				return false;
			}
		}
		return false;
	}

	public static void main(String[] args) {
		AdvCli.initRunner(args, "mutualFans  -src <sn|tx>", new MutualFollowMR());
	}
}
