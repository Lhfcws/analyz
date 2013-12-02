package com.yeezhao.analyz.filter;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.yeezhao.amkt.core.CoreConsts.USER_TYPE;
import com.yeezhao.analyz.util.AnalyzConfiguration;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.analyz.util.AppConsts.ANALYZ_OP;
import com.yeezhao.analyz.util.AppUtil;
import com.yeezhao.commons.classifier.base.Document;
import com.yeezhao.commons.hbase.HBaseUtil;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;

public class WbFpUserFilterMR implements CliRunner {
	private static final String CMD_NAME = "FpUserFilter";	
	private static final Pair<byte[], byte[]> COL_BYTES_USERTYPE = 
			HBaseUtil.getColFmlyQualifierBytes( AppConsts.COL_USR_TYPE );
	//Input columns
	private static final List<Pair<byte[], byte[]>> INPUT_FMLYCOLS = HBaseUtil
				.getColsInBytes(new String[] { 
				"fp:time" });
	
	
	static class FilterMapper extends TableMapper<Text, LongWritable>{
		
		private static enum MAP_COUNTER{
			WEIBO_COUNTER
		}
		
		public void map(ImmutableBytesWritable rowkey, Result row, Context context){
			String key = Bytes.toString(rowkey.get());
			//System.out.println(key);
			Document doc = AppUtil.row2Doc(row);
			String time = doc.containsField("fp:time")?doc.get( "fp:time" )  : "0";
			int pos = key.indexOf("wb");
			if (pos != -1) {
				String tempKey = key.substring(0, pos - 1);
				//System.out.println(tempKey);
				try {
					context.write(new Text(tempKey), new LongWritable(Long.parseLong(time)));
					context.getCounter(MAP_COUNTER.WEIBO_COUNTER).increment(1);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	static class FilterReduce extends Reducer<Text, LongWritable, Text, Text>{
		private static enum FILTER_COUNTER{
			ROBOT_COUNTER
		}
		
		HTable uInfoTbl;
		List<Put> puts = new LinkedList<Put>();
		private FpUserFilterOp userFilter=new FpUserFilterOp();
		private boolean needOpLog=false;
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int limit=conf.getInt("weibodate.limit", 180);
			userFilter.setLimit(limit);
			try {
				if( !conf.getBoolean(AppConsts.CLI_PARAM_NOWRITE, false) ){ 
					uInfoTbl = new HTable(conf, conf.get(AppConsts.HTBL_USER_PROFILE));
					System.out.println("Open table for output:" + conf.get(AppConsts.HTBL_USER_PROFILE) );
				}
				
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			if (conf.get(AdvCli.CLI_PARAM_O) != null)
				needOpLog = true;
		}

		@Override
		public void reduce(Text text, Iterable<LongWritable> values,
				Context context)
				throws IOException, InterruptedException {
				long max=0;
				Iterator<LongWritable> iters=values.iterator();
				while (iters.hasNext()) {
					long temp=iters.next().get();
					if (temp>max) {
						max=temp;
					}
				}
			
				Document doc=new Document(text.toString());
				doc.putField("lastWeiboTime", String.valueOf(max));
				String result=userFilter.classify(doc);
				System.out.println(text.toString()+", maxTime is "+max + ", result: " + result);
				if ( result.equals(Integer.toString(USER_TYPE.ROBOT.getCode()))) {
					if (needOpLog) {
						context.write(text, new Text(ANALYZ_OP.UFIL.toString()));
					}
					Put put = new Put(Bytes.toBytes(text.toString()));
					put.add(COL_BYTES_USERTYPE.getFirst(), COL_BYTES_USERTYPE.getSecond(),
							Bytes.toBytes(result ));
					context.getCounter(FILTER_COUNTER.ROBOT_COUNTER).increment(1);
					puts.add(put);
					try {
						if (uInfoTbl != null && puts.size() > AppConsts.HBASE_OP_BUF_SIZE) {
							uInfoTbl.put(puts);
							uInfoTbl.flushCommits();
							puts.clear();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
		}

		@Override
		protected void cleanup(
				Context context)
				throws IOException, InterruptedException {
			if(uInfoTbl != null){
				try {
					if(!puts.isEmpty()){
						uInfoTbl.put(puts);
						uInfoTbl.flushCommits();
						puts.clear();
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally{
					try {
						uInfoTbl.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
		
	}

	@Override
	public void start(CommandLine cmdLine) {
		Configuration conf = AnalyzConfiguration.getInstance();
		String jobName = AdvCli.genStandardJobName(CMD_NAME, cmdLine); 
		if (cmdLine.hasOption(AdvCli.CLI_PARAM_O)){
			conf.set(AdvCli.CLI_PARAM_O, cmdLine.getOptionValue(AdvCli.CLI_PARAM_O));
			System.out.println("Log output:" + cmdLine.getOptionValue(AdvCli.CLI_PARAM_O));
		}
		conf.setBoolean(AppConsts.CLI_PARAM_NOWRITE, cmdLine.hasOption(AppConsts.CLI_PARAM_NOWRITE)? true:false);
		System.out.println("Table output:" + 
				(cmdLine.hasOption(AppConsts.CLI_PARAM_NOWRITE) ? "No": conf.get(AppConsts.HTBL_USER_PROFILE)));
		try {
			Job job = new Job(conf, jobName);
			job.setJarByClass(WbUserFilterMR.class);
			
			Scan scan = new Scan();
			HBaseUtil.setScanRange(scan, cmdLine);
			HBaseUtil.addScanColumns(scan, INPUT_FMLYCOLS);		
			TableMapReduceUtil.initTableMapperJob(conf.get( AppConsts.HTBL_USER_PROFILE ), 
					scan, FilterMapper.class, Text.class, LongWritable.class, job);
			
			job.setReducerClass(FilterReduce.class);
			if (cmdLine.hasOption(AdvCli.CLI_PARAM_O)){
				FileOutputFormat.setOutputPath(job, new Path(cmdLine.getOptionValue(AdvCli.CLI_PARAM_O)));
			}else{
				job.setOutputFormatClass(NullOutputFormat.class);
			}
			job.waitForCompletion(true);

		} catch (IOException e) {
			e.printStackTrace();
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}		
	}

	@Override
	public boolean validateOptions(CommandLine cmdLine) {
		boolean result=true;
		if( cmdLine.hasOption(AdvCli.CLI_PARAM_ALL) || 
				(cmdLine.hasOption(AdvCli.CLI_PARAM_S) && cmdLine.hasOption(AdvCli.CLI_PARAM_E)))
			result= true;
		else
			result= false;
		
		
		// Need at least one output: table or output log
		if (!cmdLine.hasOption(AdvCli.CLI_PARAM_O)
				&& !cmdLine.hasOption(AppConsts.CLI_PARAM_NOWRITE)){
			result =false;
		}
		return result;
	}
	
	@SuppressWarnings("static-access")
	@Override
	public Options initOptions() {
		Options options = new Options();
		options.addOption( AdvCli.CLI_PARAM_HELP, false, "print help message");
		options.addOption( AdvCli.CLI_PARAM_ALL, false, "filter all users");		
		options.addOption(OptionBuilder.withArgName("rowkey").hasArg()
				.withDescription("start rowkey").create(AdvCli.CLI_PARAM_S));
		options.addOption(OptionBuilder.withArgName("rowkey").hasArg()
				.withDescription("end rowkey").create(AdvCli.CLI_PARAM_E));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("HDFS path for operation log").create(AdvCli.CLI_PARAM_O));		
		options.addOption(new Option(AppConsts.CLI_PARAM_NOWRITE, false, "if set, no output written to HBase"));
	
		return options;
	}

	public static void main(String[] args){	
		AdvCli.initRunner(args, CMD_NAME, new WbFpUserFilterMR() );		
	}
}
