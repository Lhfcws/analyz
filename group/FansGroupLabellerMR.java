package com.yeezhao.analyz.group;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.yeezhao.analyz.util.AnalyzConfiguration;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.analyz.util.AppUtil;
import com.yeezhao.analyz.util.AppConsts.ANALYZ_OP;
import com.yeezhao.commons.classifier.base.Document;
import com.yeezhao.commons.hbase.HBaseUtil;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;
import com.yeezhao.commons.util.StringUtil;

public class FansGroupLabellerMR implements CliRunner {
	private static final String CMD_NAME = "analyzFanGroup";	
	
	public static final Pair<byte[], byte[]> COL_BYTES_GROUP = 
			HBaseUtil.getColFmlyQualifierBytes( AppConsts.COL_USR_GROUP );

	private static class FGLMapper extends TableMapper<Text, Text> {
		public static final String USER_COUNTER_GROUP = "USER_GROUP";
		private FansGroupMdlMgr FnsGrpMdlMgr = null;
		private HTable uInfTbl = null;
		private List<Put> puts = new ArrayList<Put>();
		private boolean needOpLog = false;
		private Text rkText = new Text();
		private Text opsText = new Text(); 
		private Set<String> kwSet = new HashSet<String>();
		private boolean hasKwFilter = false;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			try {
				FnsGrpMdlMgr = new FansGroupMdlMgr(conf);
				if( !conf.getBoolean(AppConsts.CLI_PARAM_NOWRITE, false) ){ 
					uInfTbl = new HTable(conf, conf.get(AppConsts.HTBL_USER_PROFILE));
					System.out.println("Open table for output:" + conf.get(AppConsts.HTBL_USER_PROFILE) );
				}
				if (conf.get(AdvCli.CLI_PARAM_K) != null) { // Only process customized groups
					for (String kw : conf.get(AdvCli.CLI_PARAM_K).split(StringUtil.STR_DELIMIT_1ST))
						kwSet.add(kw);
					hasKwFilter = true;
				}

				if (conf.get(AdvCli.CLI_PARAM_O) != null)
					needOpLog = true;
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		public void map(ImmutableBytesWritable rowkey, Result row, Context context) {
			Document doc = AppUtil.row2Doc(row);
			try {

				doc = FnsGrpMdlMgr.process(doc);
				//取出原有group / 新assign的label
				String group = doc.containsField(AppConsts.COL_USR_GROUP)?
					doc.get(AppConsts.COL_USR_GROUP) : null;
				String label = doc.containsField(BaseGroupLabeller.LABEL)?
					doc.get(BaseGroupLabeller.LABEL) : null;

				if (hasKwFilter && !kwSet.contains(label))
					label = null;

				if (label != null) {
					Set<String> labelSet = new HashSet<String>(Arrays.asList(label.split(StringUtil.STR_DELIMIT_1ST)));
					for( String s: labelSet)
						context.getCounter(USER_COUNTER_GROUP, s).increment(1);
					
					if (needOpLog) {
						rkText.set(row.getRow());
						opsText.set( ANALYZ_OP.GROUP.toString() );
						context.write(rkText, opsText);
					}
					if (group != null)
						labelSet.addAll(Arrays.asList(group.split(StringUtil.STR_DELIMIT_1ST)));						
					
					if( uInfTbl != null ){ //Need table output
						Put put = new Put(row.getRow());
						put.add( COL_BYTES_GROUP.getFirst(), COL_BYTES_GROUP.getSecond(),
							Bytes.toBytes(StringUtil.genDelimitedString(labelSet, StringUtil.DELIMIT_1ST)));
						puts.add(put);
					}
				}

				if (uInfTbl != null && puts.size() > AppConsts.HBASE_OP_BUF_SIZE) {
					uInfTbl.put(puts);
					uInfTbl.flushCommits();
					puts.clear();
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void cleanup(Context context) {
			if (uInfTbl != null) {
				System.out.println("clean up");
				try {
					if (!puts.isEmpty()) {
						System.out.println("puts not null");
						uInfTbl.put(puts);
						uInfTbl.flushCommits();
						puts.clear();
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						uInfTbl.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	
	@SuppressWarnings("static-access")
	@Override
	public Options initOptions() {
		Options options = new Options();
		options.addOption(new Option(AdvCli.CLI_PARAM_ALL, false, "analyze all users"));
		options.addOption(new Option(AppConsts.CLI_PARAM_NOWRITE, false, "if set, no output written to HBase"));
		options.addOption(OptionBuilder.withArgName("rowkey").hasArg()
				.withDescription("start rowkey").create(AdvCli.CLI_PARAM_S));
		options.addOption(OptionBuilder.withArgName("rowkey").hasArg()
				.withDescription("end rowkey").create(AdvCli.CLI_PARAM_E));
		options.addOption(OptionBuilder.withArgName("keywords").hasArg()
				.withDescription("keywords list, i.e. <kw1>|<kw2>...")
				.create(AdvCli.CLI_PARAM_K));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("HDFS path for operation log").create(AdvCli.CLI_PARAM_O));		
		
		return options;
	}

	@Override
	public boolean validateOptions(CommandLine cmdLine) {
		if ((cmdLine.hasOption(AdvCli.CLI_PARAM_E) == true)
				&& (cmdLine.hasOption(AdvCli.CLI_PARAM_S) == false))
			return false;
		
		//Need at least one output: table or output log
		if( !cmdLine.hasOption(AdvCli.CLI_PARAM_O) && 
				!cmdLine.hasOption(AppConsts.CLI_PARAM_NOWRITE))
			return false;
		return true;
	}

	@Override
	public void start(CommandLine cmdLine) {
		Configuration conf = AnalyzConfiguration.getInstance();
		System.out.println("Input table:" + conf.get(AppConsts.HTBL_USER_PROFILE));
		
		if (cmdLine.hasOption(AdvCli.CLI_PARAM_K))
			conf.set(AdvCli.CLI_PARAM_K, cmdLine.getOptionValue(AdvCli.CLI_PARAM_K));
		if (cmdLine.hasOption(AdvCli.CLI_PARAM_O)){
			conf.set(AdvCli.CLI_PARAM_O, cmdLine.getOptionValue(AdvCli.CLI_PARAM_O));
			System.out.println("Log output:" + cmdLine.getOptionValue(AdvCli.CLI_PARAM_O));
		}
		conf.setBoolean(AppConsts.CLI_PARAM_NOWRITE, cmdLine.hasOption(AppConsts.CLI_PARAM_NOWRITE)? true:false);
		System.out.println("Table output:" + 
				(cmdLine.hasOption(AppConsts.CLI_PARAM_NOWRITE) ? "No": conf.get(AppConsts.HTBL_USER_PROFILE)));

		String jobName = AdvCli.genStandardJobName(CMD_NAME, cmdLine);
		try {
			Job job = new Job(conf, jobName);
			job.setJarByClass(FansGroupLabellerMR.class);
			job.setNumReduceTasks(0);

			Scan scan = new Scan();
			HBaseUtil.setScanRange(scan, cmdLine);
			scan.addFamily(AppConsts.COL_FMLY_FRIEND);
			if( !cmdLine.hasOption(AppConsts.CLI_PARAM_NOWRITE) )
				scan.addColumn( COL_BYTES_GROUP.getFirst(), COL_BYTES_GROUP.getSecond());

			TableMapReduceUtil.initTableMapperJob(
					conf.get(AppConsts.HTBL_USER_PROFILE), scan, FGLMapper.class,
					Text.class, Text.class, job);

			if (!cmdLine.hasOption(AdvCli.CLI_PARAM_O))
				job.setOutputFormatClass(NullOutputFormat.class);
			else {
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(NullWritable.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				FileOutputFormat.setOutputPath(job,
						new Path(cmdLine.getOptionValue(AdvCli.CLI_PARAM_O)));
			}
			job.waitForCompletion(true);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		AdvCli.initRunner(args, CMD_NAME, new FansGroupLabellerMR());
	}
}

