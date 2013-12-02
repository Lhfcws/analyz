package com.yeezhao.analyz.duanalyz;

import java.io.IOException;

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
import com.yeezhao.analyz.base.AbstractUserSyncMapper;
import com.yeezhao.analyz.base.UserAnalyzRunner;
import com.yeezhao.analyz.group.WeiboUser;
import com.yeezhao.analyz.group.processor.BaseUserProcessor;
import com.yeezhao.analyz.util.AnalyzConfiguration;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.commons.hbase.mapreduce.TableInputFormat;
import com.yeezhao.commons.hbase.mapreduce.TableMapReduceUtil;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;
import com.yeezhao.commons.util.StringUtil;

/**
 * 用户导出到sql和索引文件
 * @author yongjiang
 *
 */
public class RangeUserSync implements CliRunner, UserAnalyzRunner {
	private static final String CMD_NAME = "rangeUsrSync";

	static class RangeSyncMapper extends AbstractUserSyncMapper<ImmutableBytesWritable, Result>{
		private String analyzOps = null;
		
		@Override
		public void setup(Context context){
			super.setup(context);
			Configuration conf = context.getConfiguration();
			analyzOps = conf.get( DUConsts.PARAM_ANALYZ_OP );
			LOG.info("analyzOps: " + analyzOps);
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
			WeiboUser user = new WeiboUser();
			String src = segs[0];
			user.setWeiboType( src);
			user.setUid( rowUid);
			int weiboType = src.equals("sn") ? 1 : 2;
			String tblName = src.equals("sn") ? AppConsts.SQL_USER_SINA
					: AppConsts.SQL_USER_TX;
			user = BaseUserProcessor.setBasicUser(user, row);
			//user = BaseUserProcessor.setUserEvidence(user, row);
			syncOneUser(user, weiboType, analyzOps, tblName, context);
		}
	}
	
	@Override
	public void initMapperJob(Job job, Scan scan) throws IOException{
		job.setInputFormatClass(TableInputFormat.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapperClass(RangeSyncMapper.class);
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
		String jobName = "range sync for [" + startRow + ", " + endRow + "], analyzOps: " + analyzOps;
		
		Job job = new Job(conf, jobName);
		if (jobPool != null)
			job.getConfiguration().set(AppConsts.MAPRED_JOB_QUEUE_NAME, jobPool);
		job.getConfiguration().set( DUConsts.PARAM_ANALYZ_OP, analyzOps );
		if (outputDir != null)
			job.getConfiguration().set( CONF_OUTPUT_KEY, outputDir);
		
		job.setJarByClass(RangeUserSync.class);
		Scan scan = new Scan();
		//依赖sql字段 group_name, uid,evidence, nickname,name, gender, location_format, age
		scan.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_NICKNAME);
		scan.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_GENDER);
		scan.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_HEAD);
		scan.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_VERIFIEDTYPE);
		scan.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_FOLLOWCOUNT);
		
		scan.addColumn( AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_AGE);
		scan.addColumn( AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_LOCATION_FORMAT);
		scan.addColumn( AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_USRTYPE);
		scan.addColumn( AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_USRGROUP);
		
		//scan.addFamily( AppConsts.COL_FMLY_EVIDENCE);
		
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
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		AdvCli.initRunner(args, CMD_NAME, new RangeUserSync());
	}
}


	
