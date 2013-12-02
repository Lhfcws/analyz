package com.yeezhao.analyz.util;




import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
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
import com.yeezhao.commons.mapred.MRRunner;
import com.yeezhao.commons.mapred.MRUtil;

/**
 * Search user info table to find the incremental update operation
 * Input: user table, analyz:group_name
 * Output: HDFS file, <uid>/t<op1>|<op2>...
 * Search4IncrOp -k "k1|kw2.." [-s [startRowkey] -e [endRowkey]|-all] -o <output>
 */
public class SearchUsr4IncrOpMR implements MRRunner {
	
	private static final String CMD_NAME = "Search4IncrOp";	
	private static final String CLI_PARAM_K = "k";			//keywords parameters
	
	private static final byte[] COL_FMLY_ANALYZ = Bytes.toBytes("analyz");
	private static final byte[] COL_GROUP_NAME = Bytes.toBytes("group_name");
			
	static class Mapper extends TableMapper<Text, Text>{		
		private static final String UPDT_COUNTER_GROUP = "updt_counter";
		private String[] kws = null;
	
		private Text rkText = new Text();
		private Text opsText = new Text(); 

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			kws = conf.get(CLI_PARAM_K).split(StringUtil.STR_DELIMIT_1ST);
		}
			
		public void map(ImmutableBytesWritable rowkey, Result row, Context context){
			if( row.containsColumn(COL_FMLY_ANALYZ, COL_GROUP_NAME) ){
				String groupName = Bytes.toString( row.getValue(COL_FMLY_ANALYZ, COL_GROUP_NAME) );
				for( String kw: kws )
					if( groupName.indexOf(kw) != -1 ){
						rkText.set(rowkey.get());
						opsText.set( ANALYZ_OP.GROUP.toString() );
						try {
							context.write( rkText, opsText);
						} catch (IOException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						context.getCounter(UPDT_COUNTER_GROUP, ANALYZ_OP.GROUP.toString() ).increment(1);			
						return;
					}						
			}	
		
		}
	}

	@Override
	public void startJob( CommandLine cmdLine ) throws IOException{
		Configuration conf = AnalyzConfiguration.getInstance();
		conf.set(CLI_PARAM_K, cmdLine.getOptionValue(CLI_PARAM_K));
		String jobName = String.format( "Search user info to collect incr operations: %s",
				cmdLine.hasOption(AdvCli.CLI_PARAM_ALL)? "all": 
					cmdLine.getOptionValue(AdvCli.CLI_PARAM_S) + '-' 
					+ cmdLine.getOptionValue(AdvCli.CLI_PARAM_E) ); 
		Job job = new Job(conf, jobName);
		job.setJarByClass(SearchUsr4IncrOpMR.class);
				
		Scan scan = new Scan();
		scan.addColumn(COL_FMLY_ANALYZ, COL_GROUP_NAME);
		HBaseUtil.setScanRange(scan, cmdLine);
		
		System.out.println(String.format("Search %s in %s: ", 
				conf.get(CLI_PARAM_K), conf.get( AppConsts.HTBL_USER_PROFILE ) ) );
		
		TableMapReduceUtil.initTableMapperJob( conf.get( AppConsts.HTBL_USER_PROFILE ), 
				scan, Mapper.class, Text.class, Text.class, job);

		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path( cmdLine.getOptionValue(AdvCli.CLI_PARAM_O)));
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
				
		try {
			job.waitForCompletion(true);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}	

	public static void main(String[] args){		
		MRUtil.initMRJob(args, CMD_NAME, new SearchUsr4IncrOpMR() );		
	}
	
	@SuppressWarnings("static-access")
	@Override
	public Options initOptions() {
		Options options = new Options();
		options.addOption( AdvCli.CLI_PARAM_HELP, false, "print help message");
		options.addOption( AdvCli.CLI_PARAM_ALL, false, "scan all users' info");
		options.addOption(OptionBuilder.withArgName("keywords").hasArg()
				.withDescription("keywords list, i.e. <kw1>|<kw2>...").create(CLI_PARAM_K));
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
		if( !cmdLine.hasOption(CLI_PARAM_K) || !cmdLine.hasOption(AdvCli.CLI_PARAM_O) ) return false;	
		
		if( cmdLine.hasOption(AdvCli.CLI_PARAM_ALL) || 
				(cmdLine.hasOption(AdvCli.CLI_PARAM_S) && cmdLine.hasOption(AdvCli.CLI_PARAM_E)))
			return true;
		else
			return false;
	}	
}
