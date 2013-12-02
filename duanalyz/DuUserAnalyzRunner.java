package com.yeezhao.analyz.duanalyz;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnectionManager;

import com.yeezhao.amkt.dujob.AbstractDUJobQueue;
import com.yeezhao.amkt.dujob.DUConsts;
import com.yeezhao.amkt.dujob.DUConsts.AnalyzType;
import com.yeezhao.amkt.dujob.DUJob;
import com.yeezhao.amkt.dujob.DUJob.JobPool;
import com.yeezhao.amkt.dujob.DUJob.JobType;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.commons.util.AdvCli;

/**
 * 
 * @author yongjiang
 *
 */
public class DuUserAnalyzRunner extends AbstractAnalyzDURunner {
	Log LOG = LogFactory.getLog(DuUserAnalyzRunner.class);
	
	RangeUserAnalyz rangeUserAnalyz;
	RandomUserAnalyz randomUserAnalyz;
	
	public DuUserAnalyzRunner( AbstractDUJobQueue jobQueue ) {
		super( jobQueue );
		
		randomUserAnalyz = new RandomUserAnalyz();
		rangeUserAnalyz = new RangeUserAnalyz();
		LOG.info("new DuUserAnalyzRunner");
		System.out.println("new DuUserAnalyzRunner");
	}

	@Override
	public String getTaskType() {
		return AnalyzType.USER.name();
	}

	@Override
	protected void init(DUJob duJob) {
	}

	@Override
	public String getRunnerName() {
		return "userAnalyz";
	}

	@Override
	protected boolean doInRunJob( DUJob duJob ) {
		boolean status = true;
		try {
			String jobID = duJob.getJobID();
			String opLogPath = duJob.get( DUConsts.PARAM_OPLOG_PATH);
			JobType jobType = duJob.getJobType();
			JobPool jobPool = duJob.getJobPool();	
			String startRow = duJob.get( AdvCli.CLI_PARAM_S );
			String endRow = duJob.get( AdvCli.CLI_PARAM_E );
			String analyzOps = duJob.get( DUConsts.PARAM_ANALYZ_OP );
			String tempDataPath = duJob.get( DUConsts.PARAM_TEMPDATA_PATH );
			
			LOG.info("run userAnalyzQueue, jobID: " + jobID + ", opLogPath: " + opLogPath + ", jobType: " + jobType.name() + ", jobPool: " + jobPool.name() + 
					", analyzOps: " + analyzOps + ", tempDataPath: " + tempDataPath);		
			if (opLogPath != null ) {
				int analyzNum = randomUserAnalyz.initJob(conf, opLogPath, jobPool.name(), tempDataPath, false);
				LOG.info("randomUserAnalyzMapper waitForCompletion: " + analyzNum);
			}
			else if (analyzOps != null) {
				opLogPath = String.format(
						DUConsts.OPLOG_PATH_TEMPLATE, AppConsts.APP_CTEL, duJob.getJobID());
				status = rangeUserAnalyz.initJob(conf, analyzOps, startRow, endRow, jobPool.name(), tempDataPath);
				LOG.info("rangeUserAnalyzMapper waitForCompletion: " + status);
			}
			status = true;
			HConnectionManager.deleteConnection(conf, true);
		}
		catch (Exception e) {
			e.printStackTrace();
			status = false;
		}
		return status;
	}
}
