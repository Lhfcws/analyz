package com.yeezhao.analyz.util;





import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.commons.mapred.MRRunner;
import com.yeezhao.commons.mapred.MRUtil;
import com.yeezhao.commons.mapred.CountReducer;

/**
 * Read footprint table and update users' tweets status in user table. 
 * Input: footprint table
 * Output: user table ( crawl:tweet_total, crawl:tweet_new )
 * updtTweetStatus [-s [startRowkey] -e [endRowkey]|-all|-f [file]]
 */
public class UpdtUsrTweetsStatusMR implements MRRunner {
	private static final String CLI_PARAM_S = "s";
	private static final String CLI_PARAM_E = "e";
	private static final String CLI_PARAM_ALL = "all";
	private static final String CLI_PARAM_HELP = "help";
	
	private static final String CMD_NAME = "updtTweetStatus";
	private static final byte[] FMLY_FP = Bytes.toBytes("fp");	//footprint family
	
	//Mapper for loading input from user.profile
	static class CountMapper extends TableMapper<Text, IntWritable>{		
		private static final String WB_TYPE = AppConsts.FP_MSG;	 //weibo data type		 
		private Text uidText = new Text();
		private IntWritable one = new IntWritable(1);
		
		public void map(ImmutableBytesWritable rowkey, Result row, Context context){
			String key = Bytes.toString(rowkey.get());
			String[] keysegs = AppUtil.splitFpRowKey(key, StringUtil.STR_DELIMIT_1ST);
			if( keysegs.length != 4 ) return; 
			
			String userId = keysegs[0] + StringUtil.DELIMIT_1ST + keysegs[1];			
			if( keysegs[2].equals(WB_TYPE) ){
				uidText.set(userId);
				try {
					context.write( uidText, one);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}		
			}
		}	
	}

	static class Reducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
		private static final String UPDT_COUNTER_GROUP = "updt_counter";
		private static final byte[] COL_FMLY_CRAWL = Bytes.toBytes("crawl");
		private static final byte[] COL_TWEET_TOTAL = Bytes.toBytes("tweet_total");
		private static final byte[] COL_TWEET_NEW = Bytes.toBytes("tweet_new");
		private HTable uInfoTbl = null; 

		private static int OP_BUF_SIZE = 100;
		private List<Put> puts = new LinkedList<Put>();
		private List<Delete> deletes = new LinkedList<Delete>();

		
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			try {
				uInfoTbl = new HTable(conf, conf.get(TableOutputFormat.OUTPUT_TABLE) );
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int newTotal = 0;
			for (IntWritable val : values) newTotal += val.get();
			
			byte[] rowkeyBytes = Bytes.toBytes( key.toString() ); 
			Get get = new Get( rowkeyBytes );
			get.addColumn(COL_FMLY_CRAWL, COL_TWEET_TOTAL);
			get.addColumn(COL_FMLY_CRAWL, COL_TWEET_NEW);
			Result row = uInfoTbl.get(get);
			if( row == null ){
				System.err.println("No such row in user table: " + key );
				return;
			}

			context.getCounter(UPDT_COUNTER_GROUP, "user_with_tweets").increment(1);
			
			int oldTotal = row.containsColumn(COL_FMLY_CRAWL, COL_TWEET_TOTAL) ? 
					Integer.parseInt( Bytes.toString(row.getValue(COL_FMLY_CRAWL, COL_TWEET_TOTAL)) ): 0;
			if( newTotal != oldTotal ){
				Put put = new Put( rowkeyBytes );
				put.add(COL_FMLY_CRAWL, COL_TWEET_TOTAL, Bytes.toBytes( Integer.toString(newTotal)) );
				put.add(COL_FMLY_CRAWL, COL_TWEET_NEW, Bytes.toBytes( Integer.toString(newTotal-oldTotal)) );
				context.getCounter(UPDT_COUNTER_GROUP, "new_tweet_status_set").increment(1);
				context.write(null, put);
			}
			else{ //delete new tweet status if it exists
				if( row.containsColumn(COL_FMLY_CRAWL, COL_TWEET_NEW) ){
					Delete del = new Delete( rowkeyBytes );
					del.deleteColumn(COL_FMLY_CRAWL, COL_TWEET_NEW);
					context.write(null, del);
					context.getCounter(UPDT_COUNTER_GROUP, "new_tweet_status_deleted").increment(1);
				}
				else
					context.getCounter(UPDT_COUNTER_GROUP, "new_tweet_status_unchanged").increment(1);
			}
				
			try {
				if (puts.size() >= OP_BUF_SIZE) {
					uInfoTbl.put(puts);
					puts.clear();
				}
				if (deletes.size() >= OP_BUF_SIZE) {
					uInfoTbl.delete(deletes);
					deletes.clear();
				}
				uInfoTbl.flushCommits();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public void cleanup(Context context){
			try {
				if (puts.size() > 0) {
					uInfoTbl.put(puts);
					puts.clear();
				}
				if (deletes.size() > 0) {
					uInfoTbl.delete(deletes);
					deletes.clear();
				}
				uInfoTbl.flushCommits();
				uInfoTbl.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}		

	}
	
	public void startJob( CommandLine cmdLine ) throws IOException{
		Configuration conf = AnalyzConfiguration.getInstance();
		
		String jobName = String.format( "Update users' tweet status: %s",
				cmdLine.hasOption(CLI_PARAM_ALL)? "all": 
					cmdLine.getOptionValue(CLI_PARAM_S) + '-' + cmdLine.getOptionValue(CLI_PARAM_E) ); 
		Job job = new Job(conf, jobName);
		job.setJarByClass(UpdtUsrTweetsStatusMR.class);
				
		Scan scan = new Scan();
		scan.addFamily(FMLY_FP);
		if( !cmdLine.hasOption(CLI_PARAM_ALL) ){
			if( cmdLine.hasOption(CLI_PARAM_S) )
				scan.setStartRow(Bytes.toBytes( cmdLine.getOptionValue(CLI_PARAM_S) ));
			if( cmdLine.hasOption(CLI_PARAM_E) )
				scan.setStopRow(Bytes.toBytes( cmdLine.getOptionValue(CLI_PARAM_E)));
		}
	
		TableMapReduceUtil.initTableMapperJob( conf.get( AppConsts.HTBL_USER_PROFILE ), 
				scan, CountMapper.class, Text.class, IntWritable.class, job);		
		TableMapReduceUtil.initTableReducerJob(
				conf.get(AppConsts.HTBL_USER_PROFILE), Reducer.class, job);
		job.setCombinerClass(CountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
				
		try {
			job.waitForCompletion(true);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}	

	public static void main(String[] args){		
		MRUtil.initMRJob(args, CMD_NAME, new UpdtUsrTweetsStatusMR() );		
	}
	
	@SuppressWarnings("static-access")
	@Override
	public Options initOptions() {
		Options options = new Options();
		options.addOption( CLI_PARAM_HELP, false, "print help message");
		options.addOption( CLI_PARAM_ALL, false, "scan all users' tweets");		
		options.addOption(OptionBuilder.withArgName("rowkey").hasArg()
				.withDescription("start rowkey").create(CLI_PARAM_S));
		options.addOption(OptionBuilder.withArgName("rowkey").hasArg()
				.withDescription("end rowkey").create(CLI_PARAM_E));		
		return options;
	}

	@Override
	public boolean validateOptions(CommandLine cmdLine) {
		if( cmdLine.hasOption(CLI_PARAM_ALL) || 
				(cmdLine.hasOption(CLI_PARAM_S) && cmdLine.hasOption(CLI_PARAM_E)))
			return true;
		else
			return false;
	}	
}
