package com.yeezhao.analyz.fans;


import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.yeezhao.analyz.util.AnalyzConfiguration;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.analyz.util.AppUtil;
import com.yeezhao.commons.hbase.HBaseUtil;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;
import com.yeezhao.commons.util.FreqDist;
import com.yeezhao.commons.util.StringUtil;

/**
 * Weibo fans analysis: fanAnalyz -i <src|uid> -o <HDFS output file>
 */
public class WbFansAnalyzMR implements CliRunner {
	private static Log LOG = LogFactory.getLog(WbFansAnalyzMR.class);
	
	private static final String CMD_NAME = "fanAnalyz";

	private static final String FP_TWEET = "wb";
	private static final byte[] COL_FMLY_FP = Bytes.toBytes("fp");
	private static final byte[] COL_TWEET_TIME = Bytes.toBytes("time");
	private static final byte[] COL_WB_TERMINAL = Bytes.toBytes("weiboSource");
	private static final byte[] COL_FMLY_FANLIST = Bytes.toBytes("fanlist");
	private static final byte[] COL_FMLY_ANALYZ = Bytes.toBytes("analyz");
	
	private static final List<Pair<byte[], byte[]>> UINFO_FMLYCOLS = HBaseUtil
			.getColsInBytes(new String[] { // Columns to be analyzed
			"crawl:gender", 
			"crawl:verified",
			"crawl:followers_count",
			"analyz:age", 
			"analyz:location_format",
			"analyz:user_type",
			"analyz:group_name" });

	// load fan list from rows with the prefix <src>|<uid>|
	static class FanLoaderMap extends TableMapper<Text, DoubleWritable> {
		private static final String COUNTER_GROUP = "fans_counter";
		private static final String ATT_TWEET_HOUR = "tweet_hour";
		private static final String ATT_ACTIVE_LVL = "active_level";
		private static final String ATT_TERMINAL = "terminal";
		private static final int HOURS_OF_DAY = 24;

		private HTable uInfoTbl = null;
		private DoubleWritable one = new DoubleWritable(1.0);
		
		private ConcurrentLinkedQueue<String> fansIDQueue = new ConcurrentLinkedQueue<String>();
		private Context context;
		double[] allFanTwhrDist = null;
		FreqDist<Integer> allFanTwALvlDist = null;
		FreqDist<String> allFanTerminalDist = null;
		private ExecutorService executor;
		int threadsNum = 4;
	
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			this.context = context;
			allFanTwhrDist = new double[HOURS_OF_DAY];
			allFanTwALvlDist = new FreqDist<Integer>();
			allFanTerminalDist = new FreqDist<String>();
			
			try {
				threadsNum = Runtime.getRuntime().availableProcessors();  
				System.out.println("cpuCoreNum: " + threadsNum);
				executor = Executors.newFixedThreadPool(threadsNum);
				for (int i = 0; i < threadsNum; i++)
					executor.execute(new FansProfileReader());
					
				uInfoTbl = new HTable(conf, conf.get(AppConsts.HTBL_USER_PROFILE));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void cleanup(Context context) {
			try {
				executor.shutdown();
				uInfoTbl.close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		public void map(ImmutableBytesWritable rowkey, Result row, Context context) {
			System.out.println("Processing fans row: " + Bytes.toString(rowkey.get()));
		
			try{
				String src = AppUtil.getUsrSrc(Bytes.toString(row.getRow()));
				NavigableMap<byte[], byte[]> fansMap = row.getFamilyMap(COL_FMLY_FANLIST);
				int totalFanNum = 0;
				for (byte[] fanID : fansMap.keySet()) {				
					String usrRowkey = String.format("%s|%s",
							src, Bytes.toString(fanID));//<src|uid>
					LOG.info("usrRowKey: " + usrRowkey);
					fansIDQueue.add( usrRowkey);
					LOG.info("fansIDqueue size: " + fansIDQueue.size());
					totalFanNum++;
				}
				
				while ( !fansIDQueue.isEmpty() ) {
					try {
						Thread.sleep(5);
					}
					catch (Exception e) { e.printStackTrace(); }
				}
				LOG.info("get all fans profile, totalFanNum: " + totalFanNum);
				writeTweetDist( allFanTwALvlDist, allFanTwhrDist, allFanTerminalDist, context);			
			}
			catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
		
		private void updtFanTweetDist( int numOfTweets, List<Long> twTsList, double[] fanTwhrDist, 
				FreqDist<Integer> allFanTwALvlDist, double[] allFanTwhrDist, Context context){
			int level = AppUtil.getUsrActiveLvl(twTsList);
			allFanTwALvlDist.incr( level );
			if( level >= AppConsts.NUM_OF_ACTLVL)
				System.out.println(StringUtil.genDelimitedString(twTsList, StringUtil.DELIMIT_1ST));
								
			if( numOfTweets > 0){
				AppUtil.normalizeBySum(fanTwhrDist);
				for (int i = 0; i < HOURS_OF_DAY; i++)
					allFanTwhrDist[i] += fanTwhrDist[i];
				
				context.getCounter(COUNTER_GROUP, "fan_with_tweets").increment(1);
			}
			else
				context.getCounter(COUNTER_GROUP, "fan_no_tweets").increment(1);
		}
		
		private void collectLocalFanTwDist(Result fpRow, List<Long> twTsList, double[] fanTwhrDist, FreqDist<String> allFanTerminalDist, Context context){
			if (fpRow.containsColumn(COL_FMLY_FP, COL_TWEET_TIME)) {
				String tsString = Bytes.toString(fpRow.getValue(
						COL_FMLY_FP, COL_TWEET_TIME));
				long ts = Long.parseLong(tsString);
				twTsList.add( ts );
				fanTwhrDist[ AppUtil.getHourByTimestamp(ts) ]++;
			}
			
			if (fpRow.containsColumn(COL_FMLY_FP, COL_WB_TERMINAL) ){
				String terminal = Bytes.toString(fpRow.getValue(COL_FMLY_FP, COL_WB_TERMINAL));
				if( terminal.length() > 32 ){ //字段异常
					context.getCounter(COUNTER_GROUP, "tweet_terminal_exception").increment(1);
					terminal = AppUtil.extractWbTerminal(terminal);
					if( !terminal.equals(AppConsts.VAL_UNK) )
						context.getCounter(COUNTER_GROUP, "tweet_terminal_corrected").increment(1);
					else
						System.out.println("Terminal field err: " + terminal );								
				}
				allFanTerminalDist.incr( terminal );
			}
			else
				allFanTerminalDist.incr( AppConsts.VAL_UNK );
		}
		
		private void writeTweetDist( FreqDist<Integer> allFanTwALvlDist, double[] allFanTwhrDist, 
				FreqDist<String> allFanTerminalDist, Context context) throws IOException, InterruptedException{
			
			System.out.println( ATT_TWEET_HOUR + ":" + AppUtil.genDelimitedString(allFanTwhrDist, '|'));
			for (int i = 0; i < HOURS_OF_DAY; i++) {
				String attValStr = String.format("%s=%d", ATT_TWEET_HOUR, i);
				Text attValText = new Text();
				attValText.set(attValStr);
				DoubleWritable valWritable = new DoubleWritable();
				valWritable.set(allFanTwhrDist[i]);
				context.write(attValText, valWritable);
			}
			
			System.out.println(ATT_ACTIVE_LVL + ":" + 
					StringUtil.genDelimitedString(allFanTwALvlDist.keySet(), '|'));
			for ( Integer level: allFanTwALvlDist.keySet() ){
				String attValStr = String.format("%s=%d", ATT_ACTIVE_LVL, level );
				Text attValText = new Text();
				attValText.set( attValStr );
				DoubleWritable valWritable = new DoubleWritable();
				valWritable.set( allFanTwALvlDist.get(level) );
				context.write(attValText, valWritable);	
			}
			
			for ( String terminal: allFanTerminalDist.keySet() ){
				String attValStr = String.format("%s=%s", ATT_TERMINAL, terminal );
				Text attValText = new Text();
				attValText.set( attValStr );
				DoubleWritable valWritable = new DoubleWritable();
				valWritable.set( allFanTerminalDist.get(terminal) );
				context.write(attValText, valWritable);	
			}
		}
		/**
		 * Write attribute-val distribution into MR context
		 */
		private void writeFanAttValDist( Result usrRow, Context context) throws IOException, InterruptedException{
			for (Pair<byte[], byte[]> fmlyQual : UINFO_FMLYCOLS) {
				String valString = getValStringFromCol( usrRow, fmlyQual );
//				System.out.println(String.format("row=%s, col=%s, val=%s",
//						Bytes.toString(usrRow.getRow()),
//						Bytes.toString(fmlyQual.getSecond()),
//						valString));
				for (String v : valString.split(StringUtil.STR_DELIMIT_1ST)) {
					if( v.trim().isEmpty() ) continue;
					Text attValText = new Text();
					attValText.set(Bytes.toString(fmlyQual.getSecond()) + "=" + v);
					context.write(attValText, one);
				}
			}
		}
		
		private String getValStringFromCol( Result usrRow, Pair<byte[], byte[]> fmlyQual ){
			if( !usrRow.containsColumn(fmlyQual.getFirst(), fmlyQual.getSecond()) )
				return AppConsts.VAL_UNK;
			
			String valString = Bytes.toString(usrRow.getValue(fmlyQual.getFirst(),fmlyQual.getSecond()));
			//Non-categorical field needs further processing
			if( Bytes.toString( fmlyQual.getSecond() ).equals("followers_count") ){
				try{
					int followCount = AppUtil.getOrderMagnitude( Integer.parseInt(valString) );
					valString = followCount < 1000 ? "<1000": 
						Integer.toString(followCount);
				}
				catch( NumberFormatException ex ){
					return AppConsts.VAL_UNK;
				}
			}
			return valString;
		}
		
		class FansProfileReader implements Runnable {

			@Override
			public void run() {
				while(!Thread.interrupted()){
					String fanId = fansIDQueue.poll();
					if (fanId != null) {
						try {
							LOG.info("threadId: " + Thread.currentThread().getId()  +", begin get fanId: " + fanId);
							byte[] usrRowkey = Bytes.toBytes(fanId);
							
							List<Long> twTsList = new LinkedList<Long>();
							double[] fanTwhrDist = new double[HOURS_OF_DAY];
							Scan scan = new Scan();
							HBaseUtil.addScanColumns(scan, UINFO_FMLYCOLS); 
							scan.addColumn(COL_FMLY_FP, COL_TWEET_TIME);
							scan.addColumn(COL_FMLY_FP, COL_WB_TERMINAL);
							scan.setStartRow( usrRowkey );
							scan.setStopRow( Bytes.toBytes( HBaseUtil.getPrefixUpperBound(Bytes.toString(usrRowkey)) ) );
							int numOfTweets = 0;
										
							Result usrRow = null;
							ResultScanner resultScanner = uInfoTbl.getScanner(scan);
							while ((usrRow = resultScanner.next()) != null) {
								String uProfileRowkey = Bytes.toString(usrRow.getRow());
								if( !uProfileRowkey.contains(FP_TWEET) ){ //user info row
									context.getCounter(COUNTER_GROUP, "fans_total").increment(1);
									writeFanAttValDist( usrRow, context );
								}
								else{
									collectLocalFanTwDist(usrRow, twTsList, fanTwhrDist, allFanTerminalDist, context);
									context.getCounter(COUNTER_GROUP, "tweet_total").increment(1);
									numOfTweets++;
								}
							}
							resultScanner.close();
							updtFanTweetDist( numOfTweets, twTsList, fanTwhrDist, 
									allFanTwALvlDist, allFanTwhrDist, context);	
							LOG.info("threadId: " + Thread.currentThread().getId()  +", finish get fanId: " + fanId);
						}
						catch (Exception e) {
							e.printStackTrace();
						}
					}
					else {
						try {
							Thread.sleep(1);
						}
						catch (Exception e) { e.printStackTrace(); }
					}
				}
			}
			
		}
	}

	static class AnalyzReducer extends TableReducer<Text, DoubleWritable, ImmutableBytesWritable> {
		private byte[] uid = null;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			uid = Bytes.toBytes( conf.get(AdvCli.CLI_PARAM_I) );
		}

		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {			
			double sum = 0;
			for (DoubleWritable val : values)
				sum += val.get();
			
			Put put = new Put(uid);
			put.add( COL_FMLY_ANALYZ, Bytes.toBytes(key.toString()), 
					Bytes.toBytes(String.format("%.2f", sum)) );
			context.write(null, put);
		}
	}

	public boolean startJob(Configuration conf, String uid, String jobPool) throws IOException, InterruptedException, ClassNotFoundException {
		String jobName = String.format("Weibo fans analyzer: %s", uid);
		
		conf.set(AdvCli.CLI_PARAM_I, uid);
		Job job = new Job(conf, jobName);
		if (jobPool != null)
			job.getConfiguration().set(AppConsts.MAPRED_JOB_QUEUE_NAME, jobPool);
		job.setJarByClass(WbFansAnalyzMR.class);

		String rowPrefix = uid + StringUtil.DELIMIT_1ST;
		Scan scan = new Scan();
		scan.addFamily(COL_FMLY_FANLIST);
		scan.setStartRow(Bytes.toBytes(rowPrefix));
		scan.setStopRow(Bytes.toBytes(HBaseUtil.getPrefixUpperBound(rowPrefix)));
		
		System.out.println( String.format("Scan fans table, from %s to %s", 
				Bytes.toString(scan.getStartRow()),
				Bytes.toString(scan.getStopRow())));
		
		TableMapReduceUtil.initTableMapperJob(
				conf.get(AppConsts.HTBL_USER_FANS), scan, FanLoaderMap.class,
				Text.class, DoubleWritable.class, job);

		TableMapReduceUtil.initTableReducerJob(
				conf.get(AppConsts.HTBL_USER_FANS), AnalyzReducer.class, job);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		return job.waitForCompletion(true);
	}
	
	@Override
	public void start(CommandLine cmdLine){
		Configuration conf = AnalyzConfiguration.getInstance();
	
		try {
			String uid = cmdLine.getOptionValue(AdvCli.CLI_PARAM_I);
			startJob(conf, uid, null);
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

	@SuppressWarnings("static-access")
	@Override
	public Options initOptions() {
		Options options = new Options();
		options.addOption(AdvCli.CLI_PARAM_HELP, false, "print help message");
		options.addOption(OptionBuilder.withArgName("src|uid").hasArg()
				.withDescription("weibo uid").create(AdvCli.CLI_PARAM_I));
		return options;
	}

	@Override
	public boolean validateOptions(CommandLine cmdLine) {
		if (cmdLine.hasOption(AdvCli.CLI_PARAM_I))
			return true;
		else
			return false;
	}

	public static void main(String[] args) {
		AdvCli.initRunner(args, CMD_NAME, new WbFansAnalyzMR());
	}
}
