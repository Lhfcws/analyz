package com.yeezhao.analyz.duanalyz;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.yeezhao.amkt.core.ZkServMonitor;
import com.yeezhao.amkt.dujob.AbstractServCliRunner;
import com.yeezhao.amkt.dujob.DUConsts;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.commons.zookeeper.ZkUtil;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.zookeeper.ZkTaskManager;
import com.yeezhao.commons.zookeeper.ZooKeeperFacade;

/**
 * Command line class for monitoring GEN task queue 
 * @author yongjiang
 */
public class DUAnalyzServCli extends AbstractServCliRunner {
	private static final String CMD_NAME = "duanalyz";
	private static final String CLI_PARAM_THREADNUM = "threadNum";
	private static final int DEFAULT_THREADNUM = 5;
	private int threadNum = -1;
	
	public static void main(String[] args) throws InterruptedException {
		AdvCli.initRunner(args, CMD_NAME, new DUAnalyzServCli());
	}

	@Override
	public String getServName() {
		// TODO Auto-generated method stub
		return CMD_NAME;
	}

	@SuppressWarnings("static-access")
	@Override
	public Options initOptions() {
		Options options = new Options();
		options.addOption(AdvCli.CLI_PARAM_HELP, false, "print help message");
		options.addOption(OptionBuilder.withArgName(CLI_PARAM_THREADNUM ).hasArg()
				.withDescription( CLI_PARAM_THREADNUM ).create( CLI_PARAM_THREADNUM ));
		return options;
	}
	
	@Override
	public Configuration getConf() {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource( AppConsts.FILE_ANALYZER_CONFIG);
		return conf;
	}
	
	@Override
	public void start(CommandLine cmdLine) {
		Configuration conf = getConf();
		threadNum = DEFAULT_THREADNUM;
		String threadStr = cmdLine.getOptionValue(CLI_PARAM_THREADNUM);
		if (threadStr != null && !threadStr.isEmpty())
			threadNum = Integer.parseInt(threadStr);
		
		startService(conf, ZkServMonitor.registerService(conf, getServName()) );	
	}

	@Override
	public void startService(Configuration conf, ZooKeeperFacade zkf) {
		ZkUtil.checkAndCreateZnode(zkf, DUConsts.ZK_DU_ANALYZ );
		//Start monitoring service
		new ZkTaskManager( conf, zkf, new DUAnalyzTaskZkWatcher(zkf, threadNum), DUConsts.ZK_DU_ANALYZ );
	}
}
