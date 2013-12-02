package com.yeezhao.analyz.filter;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import com.yeezhao.analyz.util.AnalyzConfiguration;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.analyz.util.AppUtil;
import com.yeezhao.commons.classifier.base.Document;
import com.yeezhao.commons.hbase.HBaseUtil;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;

/**
 * Filter for org/robot weibo accounts
 */
public class WbUserFilterMR implements CliRunner {
	private static final String CMD_NAME = "userFilter";

	// Input columns
	private static final List<Pair<byte[], byte[]>> INPUT_FMLYCOLS = HBaseUtil
			.getColsInBytes(new String[] { AppConsts.COL_USR_NICKNAME,
					AppConsts.COL_USR_FRIENDCOUNT, AppConsts.COL_USR_FOLLOWCOUNT,
					AppConsts.COL_USR_STATUSCOUNT, AppConsts.COL_USR_TWEETNUM });

	private static final Pair<byte[], byte[]> COL_BYTES_USERTYPE = HBaseUtil
			.getColFmlyQualifierBytes(AppConsts.COL_USR_TYPE);

	static class FilterMapper extends TableMapper<Text, Text> {
		private UserFilterOp userFilter;

		HTable uInfoTbl;
		List<Put> puts = new LinkedList<Put>();

		private static final String FILTER_COUNTER_GROUP = "filter_counter";

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			try {
				uInfoTbl = new HTable(conf, conf.get(AppConsts.HTBL_USER_PROFILE));
				userFilter = new UserFilterOp(conf);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}

		public void map(ImmutableBytesWritable rowkey, Result row,
				Context context) {
			Put put = new Put(rowkey.get());
			Document doc = AppUtil.row2Doc(row);
			doc.putField(AppConsts.SRC_UID, Bytes.toString(row.getRow()));
			String userType = userFilter.classify(doc);
			put.add(COL_BYTES_USERTYPE.getFirst(),
					COL_BYTES_USERTYPE.getSecond(), Bytes.toBytes(userType));
			context.getCounter(FILTER_COUNTER_GROUP, "user_type =" + userType)
					.increment(1);
			puts.add(put);

			try {
				if (puts.size() >= AppConsts.HBASE_OP_BUF_SIZE) {
					uInfoTbl.put(puts);
					puts.clear();
				}
				uInfoTbl.flushCommits();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void cleanup(Context context) {
			try {
				if (puts.size() > 0) {
					uInfoTbl.put(puts);
					puts.clear();
				}
				uInfoTbl.flushCommits();
				uInfoTbl.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		AdvCli.initRunner(args, CMD_NAME, new WbUserFilterMR());
	}

	@SuppressWarnings("static-access")
	@Override
	public Options initOptions() {
		Options options = new Options();
		options.addOption(AdvCli.CLI_PARAM_HELP, false, "print help message");
		options.addOption(AdvCli.CLI_PARAM_ALL, false, "filter all users");
		options.addOption(OptionBuilder.withArgName("rowkey").hasArg()
				.withDescription("start rowkey").create(AdvCli.CLI_PARAM_S));
		options.addOption(OptionBuilder.withArgName("rowkey").hasArg()
				.withDescription("end rowkey").create(AdvCli.CLI_PARAM_E));

		return options;
	}

	@Override
	public boolean validateOptions(CommandLine cmdLine) {
		if (cmdLine.hasOption(AdvCli.CLI_PARAM_ALL)
				|| (cmdLine.hasOption(AdvCli.CLI_PARAM_S) && cmdLine
						.hasOption(AdvCli.CLI_PARAM_E)))
			return true;
		else
			return false;
	}

	@Override
	public void start(CommandLine cmdLine) {

		Configuration conf = AnalyzConfiguration.getInstance();
		String jobName = AdvCli.genStandardJobName(CMD_NAME, cmdLine);
		try {
			Job job = new Job(conf, jobName);
			job.setJarByClass(WbUserFilterMR.class);

			Scan scan = new Scan();
			HBaseUtil.setScanRange(scan, cmdLine);
			HBaseUtil.addScanColumns(scan, INPUT_FMLYCOLS);
			TableMapReduceUtil.initTableMapperJob(
					conf.get(AppConsts.HTBL_USER_PROFILE), scan,
					FilterMapper.class, Text.class, Text.class, job);
			job.setOutputFormatClass(NullOutputFormat.class);
			job.setNumReduceTasks(0);

			job.waitForCompletion(true);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
