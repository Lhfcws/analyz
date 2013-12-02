package com.yeezhao.analyz.util;


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.analyz.util.AppConsts.ANALYZ_OP;
import com.yeezhao.commons.hbase.HBaseUtil;
import com.yeezhao.commons.hbase.mapreduce.TableMapReduceUtil;
import com.yeezhao.commons.mapred.MRRunner;
import com.yeezhao.commons.mapred.MRUtil;

/**
 * Scan user table to collect all the analysis operations to be applied on this user.
 * Input: user table
 * Output: HDFS file, <uid>/t<op1>|<op2>...
 * collectAnalyzOp [-s [startRowkey] -e [endRowkey]|-all] -o <output>
 */
public class CollectUsrAnalyzOpMR implements MRRunner {	
	private static final String CMD_NAME = "collectAnalyzOp";
	
	private static final byte[] COL_FMLY_CRAWL = Bytes.toBytes("crawl");
	private static final byte[] COL_TWEET_NEW = Bytes.toBytes("tweet_new");
	private static final byte[] COL_LOCATION = Bytes.toBytes("location");
	private static final byte[] COL_NICKNAME = Bytes.toBytes("nickname");
	
	private static final byte[] COL_FMLY_ANALYZ = Bytes.toBytes("analyz");
	private static final byte[] COL_LOCATION_FORMAT = Bytes.toBytes("location_format");
	private static final byte[] COL_USERTYPE = Bytes.toBytes("user_type");
			
	static class Mapper extends TableMapper<Text, Text>{		
		private static final String UPDT_COUNTER_GROUP = "updt_counter";
	
		private Text rkText = new Text();
		private Text opsText = new Text(); 
			
		public void map(ImmutableBytesWritable rowkey, Result row, Context context){
			Set<String> opStrSet = new HashSet<String>();
			if( row.containsColumn(COL_FMLY_CRAWL, COL_TWEET_NEW) ){ 
				//all ops relying on tweets should be applied
				opStrSet.add( ANALYZ_OP.GROUP.toString() );
				opStrSet.add( ANALYZ_OP.AGE.toString() );
				opStrSet.add( ANALYZ_OP.WFIL.toString() );
			}
			
			if( needRedo( row, COL_FMLY_CRAWL, COL_LOCATION, COL_FMLY_ANALYZ, COL_LOCATION_FORMAT) )
				opStrSet.add( ANALYZ_OP.LOC.toString() );
			
			if( needRedo( row, COL_FMLY_CRAWL, COL_NICKNAME, COL_FMLY_ANALYZ, COL_USERTYPE ) )
				opStrSet.add( ANALYZ_OP.UFIL.toString() );
				
			
			if( !opStrSet.isEmpty() ){
				
				rkText.set(rowkey.get());
				opsText.set( StringUtil.genDelimitedString(opStrSet, StringUtil.DELIMIT_1ST) );
				try {
					context.write( rkText, opsText);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				//update counter
				for( String opStr: opStrSet )
					context.getCounter(UPDT_COUNTER_GROUP, opStr).increment(1);			
			}
		}
		
		/**
		 * Check whether a value needs to be re-analyzed
		 * 		C1: no source value, return false;
		 * 		C2: has source value but no target value, return true;
		 * 		C3: src_val.timestamp <= target_val.timestamp, return false; otherwise true
		 * @param srcFmly
		 * @param srcQual
		 * @param targetFmly
		 * @param targetQual
		 * @return true if it needs to be re-analyzed
		 */
		private boolean needRedo( Result row, byte[] srcFmly, byte[] srcQual, 
				byte[] targetFmly, byte[] targetQual ){
			KeyValue srcKv = row.getColumnLatest(srcFmly, srcQual);
			KeyValue targetKv = row.getColumnLatest(targetFmly, targetQual);
			
			if( srcKv == null ) return false; //C1
			if( targetKv == null ) return true; //C2
			if( srcKv.getTimestamp() <= targetKv.getTimestamp() ) //C3
				return false;
			else
				return true;			
		}
	}

	@Override
	public void startJob( CommandLine cmdLine ) throws IOException{
		Configuration conf = AnalyzConfiguration.getInstance();
		
		String jobName = AdvCli.genStandardJobName(CMD_NAME, cmdLine);
		Job job = new Job(conf, jobName);
		job.setJarByClass(CollectUsrAnalyzOpMR.class);
				
		Scan scan = new Scan();
		HBaseUtil.setScanRange(scan, cmdLine);
		scan.addColumn(COL_FMLY_CRAWL, COL_TWEET_NEW);
		scan.addColumn(COL_FMLY_CRAWL, COL_LOCATION);
		scan.addColumn(COL_FMLY_CRAWL, COL_NICKNAME);
		scan.addColumn(COL_FMLY_ANALYZ, COL_USERTYPE);
		scan.addColumn(COL_FMLY_ANALYZ, COL_LOCATION_FORMAT);
		
		System.out.println("Scan table: " + conf.get( AppConsts.HTBL_USER_PROFILE ) );
		
		TableMapReduceUtil.initTableMapperJob( conf.get( AppConsts.HTBL_USER_PROFILE ), 
				scan, Mapper.class, Text.class, Text.class, job);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path( cmdLine.getOptionValue(AdvCli.CLI_PARAM_O)));
		job.setNumReduceTasks(0);
				
		try {
			job.waitForCompletion(true);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}	

	public static void main(String[] args){		
		MRUtil.initMRJob(args, CMD_NAME, new CollectUsrAnalyzOpMR() );		
	}
	
	@SuppressWarnings("static-access")
	@Override
	public Options initOptions() {
		Options options = new Options();
		options.addOption( AdvCli.CLI_PARAM_HELP, false, "print help message");
		options.addOption( AdvCli.CLI_PARAM_ALL, false, "scan all users' tweets");		
		options.addOption(OptionBuilder.withArgName("rowkey").hasArg()
				.withDescription("start rowkey").create(AdvCli.CLI_PARAM_S));
		options.addOption(OptionBuilder.withArgName("rowkey").hasArg()
				.withDescription("end rowkey").create(AdvCli.CLI_PARAM_E));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output path").create(AdvCli.CLI_PARAM_O));
		return options;
	}

	@Override
	public boolean validateOptions(CommandLine cmdLine) {
		if( !cmdLine.hasOption(AdvCli.CLI_PARAM_O) ) return false;	
		
		if( cmdLine.hasOption(AdvCli.CLI_PARAM_ALL) || (cmdLine.hasOption(AdvCli.CLI_PARAM_S) 
				&& cmdLine.hasOption(AdvCli.CLI_PARAM_E)))
			return true;
		else
			return false;
	}	
}
