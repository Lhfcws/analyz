package com.yeezhao.analyz.age;

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.yeezhao.analyz.group.FootPrintWritable;
import com.yeezhao.analyz.util.AnalyzConfiguration;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.analyz.util.AppUtil;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;
import com.yeezhao.commons.util.Pair;
import com.yeezhao.commons.util.StringUtil;

//TODO grpLblMR输出的evidence现在没有做到挑选最近的N条，这涉及到ComparatorKey的设计
//TODO 怎么样将MR作为一个完整的程序在hdfs上运行？做到个别项的可配置化？
public class UserAgeLabellerMR implements CliRunner{
	private static Log LOG = LogFactory.getLog(UserAgeLabellerMR.class);
	private static final byte[] COL_FAML_FP = Bytes.toBytes("fp");
	private static final byte[] COL_TIME = Bytes.toBytes("time");
	private static final byte[] COL_CONTENT = Bytes.toBytes("content");
	private static final String NUM_DATA_ALIAS = "num.recent.data";
	private static final String CLI_NUM = "n";
	
	static class GrpLblMapper extends TableMapper<Text, FootPrintWritable>{
		private static int numRecentData = 50;
		private PriorityQueue<Pair<FootPrintWritable, String>> candQueue = null;
		private static enum MAP_COUNTER{FILTER_ROW};
		private List<String> uidWindow = new LinkedList<String>();
		
		public void setup(Context context){
			numRecentData = context.getConfiguration().getInt(NUM_DATA_ALIAS, 50);
			//保留最近时间的numRecentData条footPrint数据
			candQueue = new PriorityQueue<Pair<FootPrintWritable, String>>(numRecentData, 
					new Comparator<Pair<FootPrintWritable, String>>(){
						public int compare(Pair<FootPrintWritable, String> arg0, Pair<FootPrintWritable, String> arg1) {
							int time0 = arg0.getFirst().getPublishTime().isEmpty() ? Integer.MAX_VALUE : 
								Integer.parseInt(arg0.getFirst().getPublishTime());
							int time1 = arg1.getFirst().getPublishTime().isEmpty() ? Integer.MAX_VALUE : 
								Integer.parseInt(arg1.getFirst().getPublishTime());
							return time0 - time1;
						}});
		}
		
		public void map(ImmutableBytesWritable rowkey, Result row, Context context) {
			byte[] filterBytes = row.getValue(AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_FILTERTYPE);
			if(filterBytes != null){
				String filterValue = Bytes.toString(filterBytes);
				if(!filterValue.equals("-1")){
					context.getCounter(MAP_COUNTER.FILTER_ROW).increment(1);
					return;
				}
			}
			
			String key = Bytes.toString(rowkey.get());
			String[] keysegs = AppUtil.splitFpRowKey(key, StringUtil.STR_DELIMIT_1ST);
			String userId = keysegs[0] + StringUtil.DELIMIT_1ST + keysegs[1];
			byte[] time = row.getValue(COL_FAML_FP, COL_TIME);
			String sdTime = time == null ? "" : Bytes.toString(time); //time允许为空，用empty string(非null)表示;
			String content = Bytes.toString(row.getValue(COL_FAML_FP, COL_CONTENT));
			String type = keysegs[2]; //type不能为空
			String did = keysegs[3]; //did为空时用empty string表示
			FootPrintWritable candEvd = new FootPrintWritable(type, sdTime, content, did);
			if(!uidWindow.contains(userId)){
				if(!uidWindow.isEmpty())
					uidWindow.remove(0);
				uidWindow.add(userId);
				Pair<FootPrintWritable, String> pair = null;
				while((pair = candQueue.poll()) != null){
					try {
						context.write(new Text(pair.getSecond()), pair.getFirst());
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				candQueue.clear();
			} 
			candQueue.add(new Pair<FootPrintWritable, String>(candEvd, userId));
			if(candQueue.size() > numRecentData){
				candQueue.remove();
			}
		}
		
		public void cleanup(Context context){
			Pair<FootPrintWritable, String> pair = null;
			while((pair = candQueue.poll()) != null){
				try {
					context.write(new Text(pair.getSecond()), pair.getFirst());
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private static void startJob(int numOfMostRecentData,  String startRowkey, 
			String endRowkey) throws IOException, InterruptedException, ClassNotFoundException{
		String jobName = String.format("user age job, from row=%s, to row=%s", startRowkey, endRowkey);
		Configuration conf = AnalyzConfiguration.getInstance();
		conf.addResource( AppConsts.FILE_ANALYZER_CONFIG );
		conf.setInt(NUM_DATA_ALIAS, numOfMostRecentData);
		
		Job job = new Job(conf, jobName);
		job.setJarByClass(UserAgeLabellerMR.class);
		Scan scan = new Scan();
		scan.addColumn(COL_FAML_FP, COL_TIME);
		scan.addColumn(COL_FAML_FP, COL_CONTENT);
		
		if(startRowkey != null)
			scan.setStartRow(Bytes.toBytes(startRowkey));
		if(endRowkey != null)
			scan.setStopRow(Bytes.toBytes(endRowkey)); //1000623371
		
		LOG.info("hbase.tbl.footprint: " + conf.get("hbase.tbl.footprint"));
		TableMapReduceUtil.initTableMapperJob(conf.get("hbase.tbl.footprint"), scan, GrpLblMapper.class, 
				Text.class, FootPrintWritable.class, job);
	//	job.setReducerClass(GrpLblReducer.class);
		job.setReducerClass(AgeLblReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FootPrintWritable.class);
		job.setOutputFormatClass(NullOutputFormat.class);

		LOG.info("hbase root: " + conf.get("hbase.rootdir"));
		System.out.println("hbase root: " + conf.get("hbase.rootdir"));
		job.setNumReduceTasks(10);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void printUsage(){
		System.out.println("usage: grpLabel <numOfMostRecentData> [startRowkey] [endRowkey]");
	}
	
	@Override
	public Options initOptions() {
		Options opts = new Options();
		opts.addOption(AdvCli.CLI_PARAM_HELP, false, "print help message");
		opts.addOption(AdvCli.CLI_PARAM_ALL, false, "classify all users");
		opts.addOption(AdvCli.CLI_PARAM_S, true, "start rowkey of table yeezhao.user.profile");
		opts.addOption(AdvCli.CLI_PARAM_E, true, "end rowkey");
		opts.addOption(CLI_NUM, true, "number of most recent tweets");
		return opts;
	}

	@Override
	public boolean validateOptions(CommandLine cmdLine) {
		boolean result=true;
		if( cmdLine.hasOption(AdvCli.CLI_PARAM_ALL) || 
				(cmdLine.hasOption(AdvCli.CLI_PARAM_S) && cmdLine.hasOption(AdvCli.CLI_PARAM_E)))
			result= true;
		else
			result= false;
		if(!cmdLine.hasOption(CLI_NUM))
			result = false;
		return result;
	}

	@Override
	public void start(CommandLine cmdLine) {
		int numOfTweets = Integer.parseInt(cmdLine.getOptionValue(CLI_NUM));
		try {
			startJob(numOfTweets, cmdLine.hasOption(AdvCli.CLI_PARAM_ALL) ? null :
				cmdLine.getOptionValue(AdvCli.CLI_PARAM_S), cmdLine.hasOption(AdvCli.CLI_PARAM_ALL) ? null :
					cmdLine.getOptionValue(AdvCli.CLI_PARAM_E));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args){	
		AdvCli.initRunner(args, "age classify", new UserAgeLabellerMR() );		
	}
}
