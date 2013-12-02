package com.yeezhao.analyz.loc;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
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

import com.sjb.ontology.OntoUtil;
import com.sjb.ontology.tpclassifier.TreepathClassifierFactory;
import com.yeezhao.analyz.util.AnalyzConfiguration;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;

public class LocationFormater implements CliRunner {
	private static final byte[] COL_FAL_ANALYZ = Bytes.toBytes("analyz");
	private static final byte[] COL_LOC_FORMAT = Bytes.toBytes("location_format");
	private static final byte[] COL_FAL_CRAWL = Bytes.toBytes("crawl");
	private static final byte[] COL_LOC = Bytes.toBytes("location");
	private static final byte[] COL_HEAD = Bytes.toBytes("head");

	static class LocFormatMapper extends TableMapper<Text, Text>{
		private TreepathClassifierFactory classifier;
		private HTable userInfoTbl;
		private List<Put> puts = new LinkedList<Put>();
		private static enum MAP_COUNTER{FORMATTED_ROWS, FORMAT_SUCCESS, NOT_FETCHED};		
		
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			String knowPath = conf.getResource(conf.get("konwledge.dir")).toString();
			knowPath = knowPath.substring(knowPath.indexOf(":") + 1)
					+ File.separatorChar;
			classifier = new TreepathClassifierFactory(knowPath);
			try {
				userInfoTbl = new HTable(conf, conf.get(AppConsts.HTBL_USER_PROFILE));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		public void map(ImmutableBytesWritable key, Result row, Context context){
			byte[] head = row.getValue(COL_FAL_CRAWL, COL_HEAD);
			if(head == null)
				context.getCounter(MAP_COUNTER.NOT_FETCHED).increment(1);
			
			Map<String, String> locMap = new HashMap<String, String>();
			byte[] locbytes = row.getValue(COL_FAL_CRAWL, COL_LOC);
			if(locbytes == null)
				return;
			
			context.getCounter(MAP_COUNTER.FORMATTED_ROWS).increment(1);
			
			locMap.put(OntoUtil.WEIBO_TPCLS_ATTRIBUTES.TEXT, Bytes.toString(locbytes));
			List<String> paths = classifier.getCandidateTreepath(1001, locMap);
			String locFormat = paths.isEmpty() ? AppConsts.LOC_FORMAT_DEFAULT : paths.get(0);
			
			if(!locFormat.equals(AppConsts.LOC_FORMAT_DEFAULT))
				context.getCounter(MAP_COUNTER.FORMAT_SUCCESS).increment(1);
			
			Put put = new Put(key.get());
			put.add(COL_FAL_ANALYZ, COL_LOC_FORMAT, Bytes.toBytes(locFormat));
			puts.add(put);
			if (puts.size() >= 1000) {
				try {
					userInfoTbl.put(puts);
					userInfoTbl.flushCommits();
					puts.clear();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		public void cleanup(Context context){
			try {
				if (puts.size() > 0) {
					userInfoTbl.put(puts);
					userInfoTbl.flushCommits();
					puts.clear();
				}
				userInfoTbl.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void formatLocation(String startRowkey, String endRowkey) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = AnalyzConfiguration.getInstance();
		
		String jobName = "location formatter for user.profile";
		Job job = new Job(conf, jobName);
		
		job.setJarByClass(LocationFormater.class);
		Scan scan = new Scan();
		if(startRowkey != null)
			scan.setStartRow(Bytes.toBytes(startRowkey));
		if(endRowkey != null)
			scan.setStopRow(Bytes.toBytes(endRowkey));
		scan.addFamily(Bytes.toBytes("crawl"));
		TableMapReduceUtil.initTableMapperJob( conf.get( AppConsts.HTBL_USER_PROFILE ), scan, LocFormatMapper.class, 
				Text.class, Text.class, job);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args){	
		AdvCli.initRunner(args, "location format", new LocationFormater() );		
	}

	@Override
	public Options initOptions() {
		Options opts = new Options();
		opts.addOption(AdvCli.CLI_PARAM_HELP, false, "print help message");
		opts.addOption(AdvCli.CLI_PARAM_ALL, false, "classify all users");
		opts.addOption(AdvCli.CLI_PARAM_S, true, "start rowkey of table yeezhao.user.profile");
		opts.addOption(AdvCli.CLI_PARAM_E, true, "end rowkey");
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
		return result;
	}

	@Override
	public void start(CommandLine cmdLine) {
		try {
			formatLocation(cmdLine.hasOption(AdvCli.CLI_PARAM_ALL) ? null :
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

}
