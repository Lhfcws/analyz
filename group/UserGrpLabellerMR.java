package com.yeezhao.analyz.group;



import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.yeezhao.commons.hbase.HBaseUtil;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;
import com.yeezhao.commons.util.Pair;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.analyz.filter.WeiboFilterOp;
import com.yeezhao.analyz.util.AnalyzConfiguration;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.analyz.util.AppUtil;
import com.yeezhao.analyz.util.AppConsts.ANALYZ_OP;

//TODO grpLblMR输出的evidence现在没有做到挑选最近的N条，这涉及到ComparatorKey的设计
public class UserGrpLabellerMR implements CliRunner {
	private static Log LOG = LogFactory.getLog(UserGrpLabellerMR.class);
	private static final String CMD_NAME = "analyzGroup";	
	
	private static final String COL_FAML_EVD = "evidence";
	private static final String NUM_DATA_ALIAS = "num.recent.data";
	
	//map output: <uid, footprint>
	static class GrpLblMapper extends TableMapper<Text, FootPrintWritable>{
		private static int numRecentData = 50;
		private String lastUserId = null;
		private PriorityQueue<Pair<FootPrintWritable, String>> candQueue = null;
		
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
			String key = Bytes.toString(rowkey.get());
			String[] keysegs = AppUtil.splitFpRowKey(key, StringUtil.STR_DELIMIT_1ST);
			String userId = keysegs[0] + StringUtil.DELIMIT_1ST + keysegs[1];
			byte[] time = row.getValue(AppConsts.COL_FMLY_FP, AppConsts.COL_TIME);
			String sdTime = time == null ? "" : Bytes.toString(time); //time允许为空，用empty string(非null)表示;
			String content = Bytes.toString(row.getValue(AppConsts.COL_FMLY_FP, AppConsts.COL_CONTENT));
			String type = keysegs[2]; //type不能为空
			String did = keysegs[3]; //did为空时用empty string表示
			FootPrintWritable candEvd = new FootPrintWritable(type, sdTime, content, did);
			if(lastUserId  != null && !userId.equals(lastUserId)){
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
			lastUserId = userId;
		}
		
		public void cleanup(Context context){}
	}
	
	static class GrpLblReducer extends Reducer<Text, FootPrintWritable, Text, Text> {
	
		private GroupLabeller grpLabeller;
		private HTable usrGrpTbl;
		private List<Put> puts = new LinkedList<Put>();
		private int numEvidence;

		private Set<String> kwSet = new HashSet<String>();
		private Text rkText = new Text();
		private Text opsText = new Text(); 
		private boolean needOpLog = false;
		
		public static enum REDUCE_COUNT{
			NUM_ALL_USER, NUM_LABELED_USER, EVD_TAG_COUNT, NUM_KEY_USER
		}
		public static final String USER_COUNTER_GROUP = "USER_GROUP";
		
		public void setup(Context context){
			try {
				Configuration conf = context.getConfiguration();
				grpLabeller = new GroupLabeller(conf);
				usrGrpTbl = new HTable(context.getConfiguration(), conf.get(AppConsts.HTBL_USER_PROFILE));
				numEvidence = conf.getInt("num.evidence.pergroup", 3);
				needOpLog = conf.get(AdvCli.CLI_PARAM_O) != null ? true:false;
				if( conf.get(AdvCli.CLI_PARAM_K) != null ){ //Only process customized groups
					for( String kw: conf.get(AdvCli.CLI_PARAM_K).split(StringUtil.STR_DELIMIT_1ST) )
						kwSet.add(kw);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		public void reduce(Text key, Iterable<FootPrintWritable> values, Context context) throws IOException {
			context.getCounter(REDUCE_COUNT.NUM_ALL_USER).increment(1);
			int count = 0;
			Iterator<FootPrintWritable> itor = values.iterator();
			Map<String, UserFootPrint> fpMap = new HashMap<String, UserFootPrint>();
			while(itor.hasNext()){
				UserFootPrint fp = new UserFootPrint(itor.next());
				fpMap.put(Integer.toString(count++), fp);
			}
			Map<String, List<String>> evidents = grpLabeller.labelFootPrints(fpMap);
			
			if(evidents.isEmpty())
				return;
			context.getCounter(REDUCE_COUNT.NUM_LABELED_USER).increment(1);
			
			Iterator<String> grpItor = evidents.keySet().iterator();
			StringBuffer grps = new StringBuffer();
			boolean hasKeys = false;
			if (kwSet.isEmpty()) //没有制定keywords，代表全部需要覆盖
				hasKeys = true; 
			
			while(grpItor.hasNext()){
				String grpKey = grpItor.next();
				List<String> evdList = evidents.get(grpKey);
				context.getCounter(USER_COUNTER_GROUP, grpKey).increment(1);
				context.getCounter(USER_COUNTER_GROUP, grpKey+"_evd").increment(evdList.size());
				for(int i = 1; i < evdList.size(); i++){
					if(fpMap.get(evdList.get(i)).getDataType().equals(AppConsts.FP_TAG)){
						context.getCounter(REDUCE_COUNT.EVD_TAG_COUNT).increment(1);
						break;
					}
				}
				grps.append(grpKey).append(StringUtil.DELIMIT_1ST);
				
				if( needOpLog && kwSet.contains(grpKey) ){
					rkText.set(key);
					opsText.set( ANALYZ_OP.GROUP.toString() );
					hasKeys = true;
					try {
						context.write( rkText, opsText);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			if (!hasKeys) {
				LOG.info(key + " is not update!");
				return;
			}
			context.getCounter(REDUCE_COUNT.NUM_KEY_USER).increment(1);
			
			System.out.println("***user: " + key);
			Map<String, String> valMap = new HashMap<String, String>();
			valMap.put("analyz:group_name", grps.toString());
			for(Entry<String, List<String>> entry : evidents.entrySet()){
				String grp = entry.getKey();
				System.out.println("group: " + grp);
				for(int i = 0; i < entry.getValue().size() && i <= numEvidence; i++){
					UserFootPrint fp = fpMap.get(entry.getValue().get(i));
					StringBuffer sb = new StringBuffer();
					//column name format: <group>|<ts>|<type>|<did>
					sb.append(COL_FAML_EVD).append(":").append(grp).append(StringUtil.DELIMIT_1ST)
						.append(fp.getPublishTime()).append(StringUtil.DELIMIT_1ST)
						.append(fp.getDataType()).append(StringUtil.DELIMIT_1ST).append(fp.getId());
					
					String hkey = sb.toString();
					valMap.put(hkey, fp.getContent());
				}
			}
			Put put = AppUtil.putRow2batchUpdt(key.toString(), valMap);
			puts.add(put);
			if(puts.size() > AppConsts.HBASE_OP_BUF_SIZE){
				usrGrpTbl.put(puts);
				usrGrpTbl.flushCommits();
				puts.clear();
			}
		}
		
		public void cleanup(Context context){
			if(usrGrpTbl != null){
				try {
					if(!puts.isEmpty()){
						usrGrpTbl.put(puts);
						usrGrpTbl.flushCommits();
						puts.clear();
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally{
					try {
						usrGrpTbl.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	public static void main(String[] args) {
		AdvCli.initRunner(args, CMD_NAME, new UserGrpLabellerMR() );
	}

	@SuppressWarnings("static-access")
	@Override
	public Options initOptions() {
		Options options = new Options();
		options.addOption( AdvCli.CLI_PARAM_HELP, false, "print help message");
		options.addOption( OptionBuilder.withArgName("number").hasArg()
				.withDescription("number of recent tweets")
				.create(AdvCli.CLI_PARAM_I) );		
		options.addOption( AdvCli.CLI_PARAM_ALL, false, "analyz all users");		
		options.addOption(OptionBuilder.withArgName("rowkey").hasArg()
				.withDescription("start rowkey").create(AdvCli.CLI_PARAM_S));
		options.addOption(OptionBuilder.withArgName("rowkey").hasArg()
				.withDescription("end rowkey").create(AdvCli.CLI_PARAM_E));
		options.addOption(OptionBuilder.withArgName("keywords").hasArg()
				.withDescription("keywords list, i.e. <kw1>|<kw2>...").create(AdvCli.CLI_PARAM_K));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("operation log").create(AdvCli.CLI_PARAM_O));
	
		return options;
	}

	@Override
	public boolean validateOptions(CommandLine cmdLine) {
		if( !cmdLine.hasOption(AdvCli.CLI_PARAM_I) ) return false;
		
		if( cmdLine.hasOption(AdvCli.CLI_PARAM_ALL) || 
				(cmdLine.hasOption(AdvCli.CLI_PARAM_S) && cmdLine.hasOption(AdvCli.CLI_PARAM_E)))
			return true;
		else
			return false;		
	}

	@Override
	public void start(CommandLine cmdLine) {
		try {
			String jobName = AdvCli.genStandardJobName(CMD_NAME, cmdLine);		
			Configuration conf = AnalyzConfiguration.getInstance();
			conf.setInt("hbase.client.scanner.caching", 50);
			conf.setInt(NUM_DATA_ALIAS, Integer.parseInt(cmdLine.getOptionValue(AdvCli.CLI_PARAM_I)) );
			if( cmdLine.hasOption(AdvCli.CLI_PARAM_O) )
				conf.set(AdvCli.CLI_PARAM_O, cmdLine.getOptionValue(AdvCli.CLI_PARAM_O));
			if( cmdLine.hasOption(AdvCli.CLI_PARAM_K) )
				conf.set(AdvCli.CLI_PARAM_K, cmdLine.getOptionValue(AdvCli.CLI_PARAM_K));

			Job job = new Job(conf, jobName);
			job.setJarByClass(UserGrpLabellerMR.class);
			Scan scan = new Scan();
			scan.addColumn(AppConsts.COL_FMLY_FP, AppConsts.COL_TIME);
			scan.addColumn(AppConsts.COL_FMLY_FP, AppConsts.COL_CONTENT);
			scan.addColumn(AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_FILTERTYPE);
			FilterList filterList = new FilterList(); 
			filterList.addFilter( new SingleColumnValueFilter(
					AppConsts.COL_FMLY_FP,
					AppConsts.COL_FILTERTYPE,
			        CompareOp.EQUAL,             
			        Bytes.toBytes( WeiboFilterOp.WB_TYPE.AD.toString() )));
			filterList.addFilter( new SingleColumnValueFilter(
					AppConsts.COL_FMLY_ANALYZ,
					AppConsts.COL_FILTERTYPE,
			        CompareOp.EQUAL,             
			        Bytes.toBytes( WeiboFilterOp.WB_TYPE.EMPTY.toString())));
			scan.setFilter(filterList);
			HBaseUtil.setScanRange(scan, cmdLine);
			
			TableMapReduceUtil.initTableMapperJob( conf.get(AppConsts.HTBL_USER_PROFILE), 
					scan, GrpLblMapper.class, Text.class, FootPrintWritable.class, job);
			job.setReducerClass(GrpLblReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FootPrintWritable.class);
			if( cmdLine.hasOption(AdvCli.CLI_PARAM_O) ){
				job.setOutputFormatClass(TextOutputFormat.class);
				FileOutputFormat.setOutputPath(job, new Path( cmdLine.getOptionValue(AdvCli.CLI_PARAM_O)));
			}
			else
				job.setOutputFormatClass(NullOutputFormat.class);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
		catch(NumberFormatException e){
			System.out.println("incorrect integer format for parameter -i.");
		} 
		catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
	}	
}
