package com.yeezhao.analyz.util;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.yeezhao.commons.hbase.AppConsts;
import com.yeezhao.commons.util.StringUtil;

public class GetUser {
	// tags: 用户标签，可输入多个，只要匹配到一个就成功。
	// verified_type：认证类型(int, 对应crawl:verified_type字段）
	// minfans：最小粉丝数
	// limit：返回的用户数量上限
	private static Options initOption() {
		Options opts = new Options();
		opts.addOption("tg", true, "tags: 用户标签，可输入多个，只要匹配到一个就成功。");
		opts.addOption("vt", true,
				"verified_type:认证类型(int, 对应crawl:verified_type字段）");
		opts.addOption("mnfns", true, "minfans：最小粉丝数");
		opts.addOption("lmt", true, "limit：返回的用户数量上限");
		opts.addOption(AppConsts.CLI_PARAM_O, true, "输出地址");
		opts.addOption(AppConsts.CLI_PARAM_S, true, "start row key");
		opts.addOption(AppConsts.CLI_PARAM_E, true, "end row key");
		return opts;
	}

	private static boolean validOptions(CommandLine cmdLine) {
		if (!cmdLine.hasOption("tg"))
			return false;
		if(!cmdLine.hasOption(AppConsts.CLI_PARAM_O))
			return false;
		if ((cmdLine.hasOption(AppConsts.CLI_PARAM_E) == true)
				&& (cmdLine.hasOption(AppConsts.CLI_PARAM_S) == false))
			return false;
		return true;
	}

	public static void main(String[] args) throws ParseException, IOException {
		Options opts = initOption();
		CommandLineParser parser = new PosixParser();
		CommandLine cmdLine = parser.parse(opts, args);
		if (validOptions(cmdLine)) {
			StringBuilder jobName = new StringBuilder("Get user: ");
			Configuration conf = AnalyzConfiguration.getInstance();
			conf.addResource( com.yeezhao.analyz.util.AppConsts.FILE_ANALYZER_CONFIG );
			for (String str : args) {
				jobName.append(str).append(" ");
			}
			Job job = new Job(conf, jobName.toString());
			job.setJarByClass(GetUser.class);

			if (cmdLine.hasOption("tg"))
				conf.set("tag", cmdLine.getOptionValue("tg"));
			if (cmdLine.hasOption("vt"))
				conf.set("type", cmdLine.getOptionValue("vt"));

			if (cmdLine.hasOption("mnfns"))
				conf.set("minfans", cmdLine.getOptionValue("mnfns"));
			if (cmdLine.hasOption("lmt"))
				conf.set("limit", cmdLine.getOptionValue("lmt"));

			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes("analyz"));
			scan.addFamily(Bytes.toBytes("crawl"));
			if (cmdLine.hasOption(AppConsts.CLI_PARAM_S)) {
				scan.setStartRow(Bytes.toBytes(cmdLine
						.getOptionValue(AppConsts.CLI_PARAM_S)));
				scan.setStopRow(Bytes.toBytes(cmdLine
						.getOptionValue(AppConsts.CLI_PARAM_E)));
			}
			TableMapReduceUtil.initTableMapperJob( conf.get( com.yeezhao.analyz.util.AppConsts.HTBL_USER_PROFILE ), scan,
					SearchUser.SearchMapper.class, Text.class,
					NullWritable.class, job);

			job.setNumReduceTasks(0);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			FileOutputFormat.setOutputPath(job,
					new Path(cmdLine.getOptionValue(AppConsts.CLI_PARAM_O)));

			try {
				job.waitForCompletion(true);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		} else {
			HelpFormatter help = new HelpFormatter();
			help.printHelp("Options", opts);
		}
	}
}

class SearchUser {
	static class SearchMapper extends TableMapper<Text, NullWritable> {
		private Text rk = new Text();
		private String[] tags = null;
		private String type;
		private int mnfns = 0;	//最小粉丝数量默认为0
		private int lmt = -1;	

		private enum Counter {
			USERAMOUNT
		};

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			String tag = conf.get("tag");
			if (tag != null)
				tags = tag.split(StringUtil.STR_DELIMIT_1ST);
			type = conf.get("type");
			if (conf.get("minfans") != null)
				mnfns = Integer.parseInt(conf.get("minfans"));
			if (conf.get("limit") != null)
				lmt = Integer.parseInt(conf.get("limit"));
		}

		public void map(ImmutableBytesWritable rowkey, Result row,
				Context context) {

			// 处理用户类别
			String tmptype = Bytes.toString(row.getValue(
					Bytes.toBytes("crawl"), Bytes.toBytes("verified_type")));
			// 处理粉丝数量
			String tmnum = Bytes.toString(row.getValue(Bytes.toBytes("crawl"),
					Bytes.toBytes("followers_count")));
			if ((tmnum == null) || (tmptype == null))
				return;
			if ((type != null) && (!tmptype.equals(type)))
				return;
			int fansnum = Integer.parseInt(tmnum);
			if (fansnum < mnfns)
				return;

			// 处理关键字
			byte[] tag = row.getValue(Bytes.toBytes("crawl"),
					Bytes.toBytes("classifier_tag"));

			boolean valid = true;
			if (tags != null) {
				if (tag == null)
					return;
				String userTags = Bytes.toString(tag);
				valid = false;
				for (String tg : tags) {
					if (userTags.indexOf(tg) != -1) {
						valid = true;
						break;
					}
				}
			}
			if (!valid)
				return;

			if ((lmt != -1 && context.getCounter(Counter.USERAMOUNT).getValue() < lmt)
					|| (lmt == -1)) {
				if (Bytes.toString(row.getRow()).split(
						StringUtil.STR_DELIMIT_1ST).length < 2)
					return;
				rk.set(Bytes.toString(row.getRow()).split(
						StringUtil.STR_DELIMIT_1ST)[1]);
				try {
					context.write(rk, NullWritable.get());
					context.getCounter(Counter.USERAMOUNT).increment(1);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}
}