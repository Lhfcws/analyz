package com.yeezhao.analyz.duanalyz;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.yeezhao.amkt.dujob.DUConsts;
import com.yeezhao.analyz.base.AbstractUserAnalyzMapper;
import com.yeezhao.analyz.base.UserAnalyzRunner;
import com.yeezhao.analyz.group.UserFootPrint;
import com.yeezhao.analyz.group.WeiboUser;
import com.yeezhao.analyz.group.processor.BaseUserProcessor;
import com.yeezhao.analyz.util.AnalyzConfiguration;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.analyz.util.AppUtil;
import com.yeezhao.commons.hbase.mapreduce.TableInputFormat;
import com.yeezhao.commons.hbase.mapreduce.TableMapReduceUtil;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;
import com.yeezhao.commons.util.StringUtil;

/**
 * 连续用户分析
 * @author yongjiang
 *
 */
public class RangeUserAnalyz implements CliRunner, UserAnalyzRunner {
	private static final String CMD_NAME = "rangeUsrAnalyz";
	
	public static class RangeUserAnalyzMapper extends AbstractUserAnalyzMapper<ImmutableBytesWritable, Result> {
	
		private WeiboUser user = null;
		private Map<String, UserFootPrint> fpMap = null;
		private String uid = null;
		private String src = null;
		private int weiboType = 0;
		private String tblName = null;
		private String analyzOps = null;
		HashSet<String> prefixUids = null;    //其它uid前缀的uid
		
		@Override
		public void setup(Context context){
			super.setup(context);
			Configuration conf = context.getConfiguration();
			user = new WeiboUser();
			fpMap = new HashMap<String, UserFootPrint>();
			analyzOps = conf.get( DUConsts.PARAM_ANALYZ_OP );
			prefixUids = new HashSet<String>();
			LOG.info("analyzOps: " + analyzOps);
		}
		
		public void cleanup(Context context){
			try{
				if (uid != null) {
					LOG.info("uid: " + uid + ", profile ready!");
					analyzOneUser(user, weiboType, analyzOps, tblName, context);
					clearLastUser();
				}		
				
				if(userProfileTbl != null){
					userProfileTbl.close();
				}	
				if (hasOutput) {
					sqlOS.close();
					objOS.close();
				}
			} catch(Exception e){
				e.printStackTrace();
			}
		}
		
		@Override
		public void map(ImmutableBytesWritable rowkey, Result row, Context context){
			String rowKey = Bytes.toString( row.getRow() );	
			String[] segs = rowKey.split(StringUtil.STR_DELIMIT_1ST);
			if (segs.length < 2) {
				LOG.info(rowKey + " is illegal!");
				context.getCounter(MAPPER_COUNTER.INVALID).increment(1);
				return;
			}
			String rowUid = segs[1];
			if (prefixUids.contains(rowUid)) {
				LOG.info("prefixUid contains: " + rowKey);
				return;
			}
			
			if (uid == null || rowUid.equals( uid )) {
				readOneUser(row, rowKey, rowUid, segs);
			}
			else { //last uid profile ready
				if (rowUid.startsWith(uid)) {
					LOG.info("uid: " + uid + ", is prefix to : " + rowUid);
					if (!prefixUids.contains(uid)) {
						prefixUids.add(uid);
						context.getCounter(MAPPER_COUNTER.PREFIX_INVALID).increment(1);
						user.setUid( uid );
						user.setWeiboType( src );
						try {						
							fpMap = BaseUserProcessor.readFootprints(userProfileTbl, user);
						} catch (Exception e) {
							e.printStackTrace();
						}
						LOG.info("fpMap size: " + fpMap.size());
					}
				}		
				user.setUid( uid );
				user.setWeiboType( src );
				user.setFootPrints(fpMap);
				analyzOneUser(user, weiboType, analyzOps, tblName, context);
				
				//read new user profile
				clearLastUser();
				readOneUser(row, rowKey, rowUid, segs);
			}
		}
		
		/**
		 * 清空上一个user数据
		 */
		private void clearLastUser() {
			fpMap.clear();
			user = new WeiboUser();
		}
		
		/**
		 * 读取当前用户数据
		 * @param row
		 * @param rowKey
		 * @param rowUid
		 * @param segs
		 */
		private void readOneUser( Result row, String rowKey, String rowUid, String[] segs) {
			if (segs.length > 2) {
				String id = "";
				if (segs.length > 3)
					id = segs[3];
				//LOG.info(rowKey + ", id: " + id);
				String pbTime = AppUtil.getColumnValue(row, AppConsts.COL_FMLY_FP, AppConsts.COL_TIME);
				String content = AppUtil.getColumnValue(row, AppConsts.COL_FMLY_FP, AppConsts.COL_CONTENT);
				UserFootPrint fp = new UserFootPrint(segs[2], pbTime, content, id);
				fpMap.put(rowKey, fp);
			}
			else {
				user = BaseUserProcessor.setBasicUser(user, row);
			}
			src = segs[0];
			uid = rowUid;
			weiboType = src.equals("sn") ? 1 : 2;
			tblName = src.equals("sn") ? AppConsts.SQL_USER_SINA
					: AppConsts.SQL_USER_TX;
		}
	}
	

	@Override
	public void initMapperJob(Job job, Scan scan) throws IOException{
		job.setInputFormatClass(TableInputFormat.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapperClass(RangeUserAnalyzMapper.class);
		job.getConfiguration().set(TableInputFormat.INPUT_TABLE, job.getConfiguration().get(AppConsts.HTBL_USER_PROFILE));
		job.getConfiguration().set(TableInputFormat.SCAN,
		      TableMapReduceUtil.convertScanToString(scan));
		TableMapReduceUtil.addDependencyJars(job);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		job.setNumReduceTasks(0);

	}
	
	public boolean initJob(Configuration conf, String analyzOps, String startRow, String endRow, String jobPool, String outputDir) throws IOException, InterruptedException, ClassNotFoundException{
		String jobName = "range analyzer for [" + startRow + ", " + endRow + "], analyzOps: " + analyzOps;
		
		Job job = new Job(conf, jobName);
		if (jobPool != null)
			job.getConfiguration().set(AppConsts.MAPRED_JOB_QUEUE_NAME, jobPool);
		job.getConfiguration().set( DUConsts.PARAM_ANALYZ_OP, analyzOps );
		if (outputDir != null)
			job.getConfiguration().set( CONF_OUTPUT_KEY, outputDir);
	
		job.setJarByClass(RangeUserAnalyz.class);
		Scan scan = new Scan();
		scan.addColumn( AppConsts.COL_FMLY_FP, AppConsts.COL_TIME);
		scan.addColumn( AppConsts.COL_FMLY_FP, AppConsts.COL_CONTENT);
		scan.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_NICKNAME);
		scan.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_LOCATION);	
		scan.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_GENDER);
		scan.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_HEAD);
		scan.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_VERIFIEDTYPE);
		
		scan.addColumn( AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_AGE);
		scan.addColumn( AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_LOCATION_FORMAT);
		scan.addColumn( AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_USRTYPE);
		scan.addColumn( AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_USRGROUP);
		
		//僵尸分析依赖字段
		scan.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_FRIENDCOUNT);
		scan.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_FOLLOWCOUNT);
		scan.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_STATUSCOUNT);
		scan.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_TWEETNUM);
		
		if (startRow != null)
			scan.setStartRow( Bytes.toBytes( startRow ));
		if (endRow != null)
			scan.setStopRow( Bytes.toBytes( endRow ));
		
		initMapperJob(job, scan);
		return job.waitForCompletion(true);
	}
	
	@SuppressWarnings("static-access")
	@Override
	public Options initOptions() {
		Options options = new Options();
		options.addOption(AdvCli.CLI_PARAM_HELP, false, "print help message");
		options.addOption(OptionBuilder.withArgName("rowkey").hasArg()
				.withDescription("start rowkey").create(AdvCli.CLI_PARAM_S));
		options.addOption(OptionBuilder.withArgName("rowkey").hasArg()
				.withDescription("end rowkey").create(AdvCli.CLI_PARAM_E));
		options.addOption(OptionBuilder.withArgName("output").hasArg()
				.withDescription("output").create(AdvCli.CLI_PARAM_O));
		
		options.addOption(OptionBuilder.withArgName("oplist").hasArg()
				.withDescription("Analyz op list").create(DUConsts.PARAM_ANALYZ_OP));	
		return options;
	}

	@Override
	public boolean validateOptions(CommandLine cmdLine) {
		return cmdLine.hasOption( DUConsts.PARAM_ANALYZ_OP );
	}

	@Override
	public void start(CommandLine cmdLine) {
		Configuration conf = AnalyzConfiguration.getInstance();
		
		try {
			initJob(conf, cmdLine.getOptionValue( DUConsts.PARAM_ANALYZ_OP ), cmdLine.getOptionValue( AdvCli.CLI_PARAM_S),
					cmdLine.getOptionValue( AdvCli.CLI_PARAM_E), null, cmdLine.getOptionValue( AdvCli.CLI_PARAM_O ));
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		AdvCli.initRunner(args, CMD_NAME, new RangeUserAnalyz());
	}
}
