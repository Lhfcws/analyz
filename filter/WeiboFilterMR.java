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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import com.yeezhao.analyz.filter.WeiboFilterOp.WB_TYPE;
import com.yeezhao.analyz.util.AnalyzConfiguration;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.commons.hbase.HBaseUtil;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;

public class WeiboFilterMR implements CliRunner {
	private static final String CMD_NAME = "weiboFilter";
	
	static class FilterMapper extends TableMapper<Text, Text>{
		WeiboFilterOp filter;
		HTable ftpTbl;
		List<Put> puts = new LinkedList<Put>();
		private static final String FILTER_COUNTER_GROUP = "filter_counter";
		
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			try {
				ftpTbl = new HTable(conf, conf.get(AppConsts.HTBL_USER_PROFILE));
				filter = new WeiboFilterOp(context.getConfiguration());			
			} catch (IOException e) {
				e.printStackTrace();
			}			
		}
		
		public void map(ImmutableBytesWritable rowkey, Result row, Context context){
			String content = Bytes.toString(row.getValue(AppConsts.COL_FMLY_FP, AppConsts.COL_CONTENT));
			
			WB_TYPE wbType = filter.classify(content); 
			context.getCounter(FILTER_COUNTER_GROUP, wbType.toString() ).increment(1);
			
			//Only write the type that needs to be filtered
			if( !wbType.equals(WB_TYPE.NORMAL) ){ 
				Put put = new Put(rowkey.get());
				put.add(AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_FILTERTYPE, 
						Bytes.toBytes(Integer.toString(wbType.getCode())));
				puts.add(put);
				if( puts.size() >= AppConsts.HBASE_OP_BUF_SIZE ){
					try {
						ftpTbl.put(puts);
						ftpTbl.flushCommits();
						puts.clear();
					} catch (IOException e) {
						e.printStackTrace();
					}
					
				}
			}
		}
		
		public void cleanup(Context context){
			try {
				if(!puts.isEmpty()){
						ftpTbl.put(puts);
						ftpTbl.flushCommits();
						puts.clear();
				}
				ftpTbl.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}	
	}
	
	public static void main(String[] args) throws IOException {
		AdvCli.initRunner(args, CMD_NAME, new WeiboFilterMR() );
	}

	@Override
	public void start(CommandLine cmdLine) {
		Configuration conf = AnalyzConfiguration.getInstance();
		
		try {
			String jobName = AdvCli.genStandardJobName(CMD_NAME, cmdLine);
			Job job = new Job(conf, jobName);
			job.setJarByClass(WeiboFilterMR.class);
			
			Scan scan = new Scan();
			HBaseUtil.setScanRange(scan, cmdLine);
			scan.addColumn(AppConsts.COL_FMLY_FP, AppConsts.COL_CONTENT);
			TableMapReduceUtil.initTableMapperJob(conf.get(AppConsts.HTBL_USER_PROFILE), 
					scan, FilterMapper.class, Text.class, Text.class, job);
			job.setOutputFormatClass(NullOutputFormat.class);
			job.setNumReduceTasks(0);
		
			job.waitForCompletion(true);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	@SuppressWarnings("static-access")
	@Override
	public Options initOptions() {
		Options options = new Options();
		options.addOption( AdvCli.CLI_PARAM_HELP, false, "print help message");
		options.addOption( AdvCli.CLI_PARAM_ALL, false, "filter all weibo");		
		options.addOption(OptionBuilder.withArgName("rowkey").hasArg()
				.withDescription("start rowkey").create(AdvCli.CLI_PARAM_S));
		options.addOption(OptionBuilder.withArgName("rowkey").hasArg()
				.withDescription("end rowkey").create(AdvCli.CLI_PARAM_E));
	
		return options;
	}

	@Override
	public boolean validateOptions(CommandLine cmdLine) {
		if( cmdLine.hasOption(AdvCli.CLI_PARAM_ALL) || 
				(cmdLine.hasOption(AdvCli.CLI_PARAM_S) && cmdLine.hasOption(AdvCli.CLI_PARAM_E)))
			return true;
		else
			return false;
	}
	
}
