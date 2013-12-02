package com.yeezhao.analyz.util;

import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.yeezhao.amkt.core.TableSplitUtil;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;

public class SinaNameUpdate implements CliRunner {
	private static Log LOG = LogFactory.getLog(SinaNameUpdate.class);
	
	protected static final String CONF_OUTPUT_KEY = "output.dir";
	private static final String UPDATE_SUFFIX = "_update.sql";
	private static final String DELETE_SUFFIX = "_delete.sql";
	private static final String CRAWL_SUFFIX = "_crawl.txt";
	private static final String CHECK_SUFFIX = "_check.txt";
	
	protected static enum MAPPER_COUNTER {
		INVALID, NO_NAME, CRAWL, UPDATE, DELETE, OUTTIME, CHECK
		}	
	
	protected static final String UPDTTIME_CHANGE_GROUP = "UPDTTIME_CHANGE";
	protected static final String UPDTTIME_NOCHANGE_GROUP = "UPDTTIME_NOCHANGE";
	
	private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	
	public static class ReadMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		private HTable profileHTable;
		private OutputStream updateOS;
		private OutputStream crawlOS;
		private OutputStream deleteOS;
		private OutputStream checkOS;
		private int timeDistance = 37 * 24 * 3600;
		private int curTime = (int) (System.currentTimeMillis() / 1000);
		
		@Override
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			try {
				profileHTable = new HTable(conf, conf.get(AppConsts.HTBL_USER_PROFILE));
				String filePrefix = context.getTaskAttemptID().toString();
				updateOS = FileSystem.get(conf).create(new Path(new Path(conf
						.get(CONF_OUTPUT_KEY)), filePrefix
						+ UPDATE_SUFFIX));
				deleteOS = FileSystem.get(conf).create(new Path(new Path(conf
						.get(CONF_OUTPUT_KEY)), filePrefix
						+ DELETE_SUFFIX));
				crawlOS = FileSystem.get(conf).create(new Path(new Path(conf
						.get(CONF_OUTPUT_KEY)), filePrefix
						+ CRAWL_SUFFIX));
				checkOS = FileSystem.get(conf).create(new Path(new Path(conf
						.get(CONF_OUTPUT_KEY)), filePrefix
						+ CHECK_SUFFIX));
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void cleanup(Context context){
			try {
				profileHTable.close();
				crawlOS.close();
				deleteOS.close();
				updateOS.close();
				checkOS.close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void map(LongWritable offset, Text line, Context context){
			if (line.toString().isEmpty())
				return;
			String row = line.toString().trim();
			int pos = row.indexOf("	");
			if (pos <= 0) {
				LOG.info(row + ", is error!");
				return;
			}
			String uid = row.substring(0, pos).trim();
			if (uid.isEmpty()) {
				LOG.info(row + ", is error2!");
				return;
			}
			String oldNickname = row.substring(pos+1).trim();
			String uidKey = "sn|" + uid;
			try {
				Get get = new Get(Bytes.toBytes(uidKey));
				get.addColumn(AppConsts.COL_FMLY_CRAWL, AppConsts.COL_NICKNAME);
				get.addColumn(AppConsts.COL_FMLY_STATUS, AppConsts.COL_FETCH);
				get.addColumn(AppConsts.COL_FMLY_CRAWL, AppConsts.COL_FOLLOWCOUNT);
				
				Result rs = profileHTable.get(get);
				String newNickname = null;
				KeyValue kv = rs.getColumnLatest(AppConsts.COL_FMLY_CRAWL, AppConsts.COL_NICKNAME);
				String timeFormat = null;
				int uidTime = -1;
				int followcount = -1; 
				if (kv != null && kv.getValue() != null) {
					newNickname = Bytes.toString(kv.getValue()).trim();
					Date date = new Date(kv.getTimestamp());
					timeFormat = simpleDateFormat.format(date);
					uidTime = (int)(date.getTime()/1000);
				}
				
				if (rs.getValue(AppConsts.COL_FMLY_CRAWL, AppConsts.COL_FOLLOWCOUNT) != null)
				{
					try {
						followcount = Integer.parseInt(Bytes.toString(rs.getValue(AppConsts.COL_FMLY_CRAWL, AppConsts.COL_FOLLOWCOUNT)));
					}
					catch (Exception e) {
						e.printStackTrace();
						followcount = -1;
					}
				}
				
				String status = AppUtil.getColumnValue(rs, AppConsts.COL_FMLY_STATUS, AppConsts.COL_FETCH, "1");
				String sqlExpr = null;
				
				int tableNum = TableSplitUtil.getTableSplitNum(uid);
				String splitTableName = String.format("%s_%s", AppConsts.SQL_USER_SINA,
						tableNum);
				
				if (status.equals("3")) {
					if (followcount < 1000) {
						sqlExpr = String.format("delete from %s where uid='%s'",
								splitTableName, uid);
						context.getCounter(MAPPER_COUNTER.DELETE).increment(1);
						deleteOS.write(Bytes.toBytes(sqlExpr + ";\n"));
					}
					else {
						context.getCounter(MAPPER_COUNTER.CHECK).increment(1);
						checkOS.write(Bytes.toBytes(uid + "|1\n"));
					}
				}
				else {
					if (newNickname != null) {
						if (newNickname.equals(oldNickname)) {
							context.getCounter(UPDTTIME_NOCHANGE_GROUP, timeFormat).increment(1);
						}
						else {
							LOG.info("uid: " + uid + ", new: " + newNickname + ", " + oldNickname);
							context.getCounter(UPDTTIME_CHANGE_GROUP, timeFormat).increment(1);
							context.getCounter(MAPPER_COUNTER.UPDATE).increment(1);
							Map<String, String> updtValMap = new HashMap<String, String>();
							updtValMap.put(AppConsts.COL_USR_NICKNAME, newNickname);
							updtValMap.put(AppConsts.COL_USR_NAME, newNickname);
							String updtValStr = AppUtil.generateUpdtExpr(updtValMap); // only update change attribute to sql
							if (updtValStr != null) {
								sqlExpr = String.format(
										"update %s set %s where uid='%s'",
										splitTableName, updtValStr, uid);
								updateOS.write(Bytes.toBytes(sqlExpr + ";\n"));
							}
						}
						if (curTime - uidTime >= timeDistance) {
							context.getCounter(MAPPER_COUNTER.OUTTIME).increment(1);
							context.getCounter(MAPPER_COUNTER.CRAWL).increment(1);
							crawlOS.write(Bytes.toBytes(uid + "|1\n"));
						}
					}
					else {
						context.getCounter(MAPPER_COUNTER.NO_NAME).increment(1);
						context.getCounter(MAPPER_COUNTER.CRAWL).increment(1);
						crawlOS.write(Bytes.toBytes(uid + "|1\n"));
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public boolean initJob(Configuration conf, String operationsFile, String outputDir) throws IOException, InterruptedException, ClassNotFoundException{
		Job job = new Job(conf, "sina name update, input file=" + operationsFile);
		job.setJarByClass(SinaNameUpdate.class);
		job.getConfiguration().set(CONF_OUTPUT_KEY, outputDir);
		
		job.setMapperClass(ReadMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(NullOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(operationsFile));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true);
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		AdvCli.initRunner(args, "sina name update", new SinaNameUpdate());
	}

	@Override
	public Options initOptions() {
		Options opts = new Options();
		opts.addOption(AdvCli.CLI_PARAM_HELP, false, "print help message");
		opts.addOption(AdvCli.CLI_PARAM_I, true, "input hdfs file");
		opts.addOption(AdvCli.CLI_PARAM_O, true, "output hdfs path");
		return opts;
	}

	@Override
	public boolean validateOptions(CommandLine cmdLine) {
		return cmdLine.hasOption(AdvCli.CLI_PARAM_I) && cmdLine.hasOption(AdvCli.CLI_PARAM_O);
	}

	@Override
	public void start(CommandLine cmdLine) {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource( AppConsts.FILE_ANALYZER_CONFIG );
		
		try {
			initJob(conf, cmdLine.getOptionValue(AdvCli.CLI_PARAM_I), cmdLine.getOptionValue(AdvCli.CLI_PARAM_O));
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
