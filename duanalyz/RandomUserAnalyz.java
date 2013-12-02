package com.yeezhao.analyz.duanalyz;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.yeezhao.analyz.base.AbstractUserAnalyzMapper;
import com.yeezhao.analyz.base.UserAnalyzRunner;
import com.yeezhao.analyz.group.WeiboUser;
import com.yeezhao.analyz.group.processor.BaseUserProcessor;
import com.yeezhao.analyz.util.AnalyzConfiguration;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.analyz.util.AppUtil;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;
import com.yeezhao.commons.util.StringUtil;

/**
 * 随机用户分析
 * @author yongjiang
 *
 */
public class RandomUserAnalyz implements CliRunner, UserAnalyzRunner {
	private static final Log LOG = LogFactory.getLog(RandomUserAnalyz.class);
	private static final String CMD_NAME = "randomUsrAnalyz";

	public static class RandomUserAnalyzMapper extends
			AbstractUserAnalyzMapper<LongWritable, Text> {

		@Override
		public void map(LongWritable offset, Text line, Context context) {
			String[] segs = line.toString().split("\\s+");
			String[] idsegs = segs[0].split(StringUtil.STR_DELIMIT_1ST);

			if (idsegs.length < 2) {
				LOG.info("illegal row: " + line.toString());
				context.getCounter(MAPPER_COUNTER.INVALID).increment(1);
				return;
			}
			String tblName = idsegs[0].equals("sn") ? AppConsts.SQL_USER_SINA
					: AppConsts.SQL_USER_TX;
			int weiboType = idsegs[0].equals("sn") ? 1 : 2;
			String uid = idsegs[1].trim();
			WeiboUser user = new WeiboUser();
			user.setUid(uid);
			user.setWeiboType(idsegs[0].trim());
			try {
				user = BaseUserProcessor.readProfileInfo(userProfileTbl, user);
				analyzOneUser(user, weiboType, segs[1], tblName, context);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void initMapperJob(Job job, Scan scan) throws IOException {
		job.setMapperClass(RandomUserAnalyzMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(NullOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}
	
	public int initJob(Configuration conf, String operationsFile, String jobPool, String outputDir, boolean needSplit)
			throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(conf, "random analyzer, input file=" + operationsFile);
		if (jobPool != null)
			job.getConfiguration().set(AppConsts.MAPRED_JOB_QUEUE_NAME, jobPool);
		if (outputDir != null)
			job.getConfiguration().set( CONF_OUTPUT_KEY, outputDir);
		job.setJarByClass(RandomUserAnalyz.class);

		Path splitDir = new Path(operationsFile);
		FileSystem fs = FileSystem.get(conf);
		if (needSplit) {
			List<String> inputLines = new LinkedList<String>();
			FileStatus[] statuses = fs.listStatus(new Path(operationsFile));
			Path[] paths = FileUtil.stat2Paths(statuses);
			if (paths != null && paths.length > 0)
				for (Path path : paths) {
					if (!path.toString().contains("_logs")) {
						BufferedReader br = new BufferedReader(
								new InputStreamReader(fs.open(path)));
						String line = null;
						while ((line = br.readLine()) != null) {
							inputLines.add(line);
						}
						br.close();
					}
				}
			LOG.info("total input lines: " + inputLines.size());
			int mapNum = inputLines.size() / 5000;
			mapNum = mapNum < AppConsts.DEFAULT_MAP_NUM ? AppConsts.DEFAULT_MAP_NUM : mapNum;
			if (!inputLines.isEmpty()) {
				splitDir = AppUtil.generateHdfsSplits(conf,"random-analyzer-split"
								+ System.currentTimeMillis(), inputLines, mapNum);
				LOG.info("input path: " + splitDir);
			} else
				return 0;
		}
		
		initMapperJob(job, null);
		FileInputFormat.addInputPath(job, splitDir);
		
		boolean res = job.waitForCompletion(true);
		fs.delete(splitDir, true);

		return res ? 1 : -1;
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		AdvCli.initRunner(args, CMD_NAME,
				new RandomUserAnalyz());
	}

	@SuppressWarnings("static-access")
	@Override
	public Options initOptions() {
		Options opts = new Options();
		opts.addOption(AdvCli.CLI_PARAM_HELP, false, "print help message");
		opts.addOption(AdvCli.CLI_PARAM_I, true, "input hdfs file");
		opts.addOption(CLI_PARAM_NEEDSPLIT, false, "not need split input path");
		opts.addOption(OptionBuilder.withArgName("output").hasArg()
				.withDescription("output").create(AdvCli.CLI_PARAM_O));
		return opts;
	}

	@Override
	public boolean validateOptions(CommandLine cmdLine) {
		return cmdLine.hasOption(AdvCli.CLI_PARAM_I);
	}

	@Override
	public void start(CommandLine cmdLine) {
		Configuration conf = AnalyzConfiguration.getInstance();

		try {
			boolean needSplit = cmdLine.hasOption( CLI_PARAM_NEEDSPLIT) ? true : false;
			initJob(conf, cmdLine.getOptionValue(AdvCli.CLI_PARAM_I), null, cmdLine.getOptionValue(AdvCli.CLI_PARAM_O), needSplit);
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
}
