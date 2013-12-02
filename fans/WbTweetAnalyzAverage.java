package com.yeezhao.analyz.fans;

import java.io.IOException;

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
import org.apache.hadoop.hbase.util.Bytes;

import com.yeezhao.analyz.util.AnalyzConfiguration;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;
import com.yeezhao.commons.util.Pair;
import com.yeezhao.commons.util.StringUtil;

/**
 * 统计用户微博的平均转发数和平均评论数，并写入user.fans表
 * @author abekwok/yongjiang
 *
 */
public class WbTweetAnalyzAverage implements CliRunner {
	private Log LOG = LogFactory.getLog(this.getClass());
	
	private HTable fansTable;
	private HTable intracTable;
	private static boolean nowrite = false;  // 命令行指定是否写入Hbase，默认false，即要写入
		
	public void updateAverage(String rowkey) throws IOException{
		System.out.println("Calculating from table[" + Bytes.toString(intracTable.getTableName()) + "].....");
		Pair<Float,Float> res = getAverageFromHbase(rowkey);
		if (res==null){
			System.out.println("Fail to get average");
			return ;
		}
		String avg_reposts = Float.toString(res.first);
		String avg_comments = Float.toString(res.second);
		System.out.println(Bytes.toString(AppConsts.COL_FMLY_ANALYZ) + ":" + Bytes.toString(AppConsts.AVGREPOST_QUA) + " -- \t" + avg_reposts);
		System.out.println(Bytes.toString(AppConsts.COL_FMLY_ANALYZ) + ":" + Bytes.toString(AppConsts.AVGCOMMENT_QUA) + " -- \t" + avg_comments);
		
		if(!nowrite){
			System.out.println("update table: " + Bytes.toString(fansTable.getTableName()));
			Put put = new Put(Bytes.toBytes(rowkey));
			put.add(AppConsts.COL_FMLY_ANALYZ, AppConsts.AVGREPOST_QUA, Bytes.toBytes(avg_reposts));
			put.add(AppConsts.COL_FMLY_ANALYZ, AppConsts.AVGCOMMENT_QUA, Bytes.toBytes(avg_comments));
			fansTable.put(put);
			fansTable.close();
		}
		else{
			System.out.println("Command -nowrite: Results would not be written into hbase.");
		}
		System.out.println("updateAverage Done");
	}
	
	/**
	 * @param rowkey
	 * @return 《avg_repost, avg_comments》
	 * @throws IOException 
	 */
	public Pair<Float,Float> getAverageFromHbase(String rowkey) throws IOException{
		float repost_total = 0;
		float comment_total = 0;
		int weibo_count = 0;
		int skip_count = 0;
		
		String rowkeyx = rowkey + StringUtil.DELIMIT_1ST;
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(rowkeyx));
		scan.setStopRow(Bytes.toBytes(rowkeyx + "}"));
		ResultScanner scanner = intracTable.getScanner(scan);
		Result rs = null;
		String key;
		while ( (rs = scanner.next() )!= null) {
			key = Bytes.toString(rs.getRow());
			if(key.contains("|wb|")){
				int c_comment = 0;
				int c_repost = 0;
				
				// comment_total
				byte[] count = rs.getValue(AppConsts.COL_FMLY_CRAWL, AppConsts.COMMENTTOTAL_QUA);
				if(count!=null){
					c_comment = Integer.valueOf(Bytes.toString(count));
					comment_total += c_comment;
				}
				
				// repost_total
				count = null;
				count = rs.getValue(AppConsts.COL_FMLY_CRAWL, AppConsts.REPOSTTOTAL_QUA);
				if(count != null){
					c_repost = Integer.valueOf(Bytes.toString(count));
					repost_total += c_repost;
				}
				
				// 评论和转发数不全为0才加入统计，两者全为0则忽略该条微博
				if(0 != c_comment || 0 != c_repost){
					weibo_count++;
				}
				else{
					skip_count++;
				}
			}
		}
		scanner.close();
		
		System.out.println("# skip weibo num:   " + skip_count);
		System.out.println("# count weibo num:  " + weibo_count);
		System.out.println("# comment_total num:" + comment_total);
		System.out.println("# repost_total num :" + repost_total);
		comment_total = weibo_count == 0 ? comment_total : comment_total/weibo_count;
		repost_total = weibo_count == 0 ? repost_total : repost_total/weibo_count;
		return new Pair<Float,Float>(repost_total, comment_total);
	}
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		AdvCli.initRunner(args, "analyzAvg", new WbTweetAnalyzAverage() );
	}

	@SuppressWarnings("static-access")
	@Override
	public Options initOptions() {
		Options options = new Options();
		options.addOption(AdvCli.CLI_PARAM_HELP, false, "print help message");
		options.addOption(OptionBuilder.withArgName("src|uid").hasArg()
				.withDescription("target rowkey, eg: sn|1965037477").create(AdvCli.CLI_PARAM_I));
		options.addOption( AppConsts.CLI_PARAM_NOWRITE, false, "If set, results would not be written into hbase.");

		return options;
	}

	@Override
	public boolean validateOptions(CommandLine cmdLine) {
		if( cmdLine.hasOption(AdvCli.CLI_PARAM_I))
			return true;
		else
			return false;
	}

	public void close() {
		try {
			fansTable.close();
			intracTable.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void start(Configuration conf, String rowkey) {
		try {
			conf.setInt(AppConsts.HBASE_CLIENT_SCANNER_CACHING, AppConsts.HBASE_SCANNER_CACHING_SIZE);
			fansTable = new HTable(conf, conf.get(AppConsts.HTBL_USER_FANS));
			intracTable = new HTable(conf, conf.get(AppConsts.HTBL_USER_INTRAC)); 
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error("初使化表时出现异常！",e);
		} 
		
		System.out.println("Input table: " + "[" +  conf.get(AppConsts.HTBL_USER_INTRAC) + "]");
		System.out.println("Output table: " + AppConsts.HTBL_USER_FANS + "[" +  conf.get(AppConsts.HTBL_USER_FANS) + "]");
		System.out.println("Target rowkey: " + rowkey);
		
		try {
			updateAverage(rowkey);
			close();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	@Override
	public void start(CommandLine cmdLine) {
		if( cmdLine.hasOption(AppConsts.CLI_PARAM_NOWRITE) ){
			nowrite = true;
			System.out.println("Found command -nowrite. Results would not be written into hbase.");
		}
    	Configuration conf = AnalyzConfiguration.getInstance();
		try {
			fansTable = new HTable(conf, conf.get(AppConsts.HTBL_USER_FANS));
			intracTable = new HTable(conf, conf.get(AppConsts.HTBL_USER_INTRAC)); 
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error("初使化表时出现异常！",e);
		} 
		
		String rowkey = cmdLine.getOptionValue(AdvCli.CLI_PARAM_I);
		System.out.println("Input table: " + "[" +  conf.get(AppConsts.HTBL_USER_INTRAC) + "]");
		System.out.println("Output table: " + AppConsts.HTBL_USER_FANS + "[" +  conf.get(AppConsts.HTBL_USER_FANS) + "]");
		System.out.println("Target rowkey: " + rowkey);
		
		try {
			updateAverage(rowkey);
			close();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}

}
