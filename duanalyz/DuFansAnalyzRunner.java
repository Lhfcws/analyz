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
import com.yeezhao.analyz.fans.UserFansAnalyzMR;
import com.yeezhao.analyz.fans.WbTweetAnalyzAverage;
import com.yeezhao.commons.util.StringUtil;

/**
 * 
 * @author yongjiang
 *
 */
public class DuFansAnalyzRunner extends AbstractAnalyzDURunner {
	Log LOG = LogFactory.getLog(DuFansAnalyzRunner.class);
	
	private UserFansAnalyzMR userFansAnalyzMR;
	private WbTweetAnalyzAverage wbTweetAnalyzAverage;
	
	public DuFansAnalyzRunner( AbstractDUJobQueue jobQueue ) {
		super( jobQueue );
		
		userFansAnalyzMR = new UserFansAnalyzMR();
		wbTweetAnalyzAverage = new WbTweetAnalyzAverage();
		LOG.info("new DuFansAnalyzRunner");
		System.out.println("new DuFansAnalyzRunner");
	}

	@Override
	public String getTaskType() {
		return AnalyzType.FANS.name();
	}

	@Override
	protected void init(DUJob duJob) {
	}

	@Override
	public String getRunnerName() {
		return "fansAnalyz";
	}

	@Override
	protected boolean doInRunJob( DUJob duJob ) {
		boolean status = true;
		try {
			String jobID = duJob.getJobID();
			String uid = duJob.get(DUConsts.PARAM_TARGET_UID);
			String nickname = duJob.get(DUConsts.PARAM_NICKNAME);
			String opLogPath = duJob.get(DUConsts.PARAM_OPLOG_PATH);
			String src = duJob.get(DUConsts.PARAM_DATA_SRC);
			JobType jobType = duJob.getJobType();
			JobPool jobPool = duJob.getJobPool();

			LOG.info("run fansAnalyzQueue, jobID: " + jobID + ", uid: " + uid
					+ ", nickname: " + nickname + ", opLogPath: " + opLogPath
					+ ", jobType: " + jobType.name() + ", jobPool: "
					+ jobPool.name());

			if (uid != null && jobType != null) {
				// 粉丝更新
				String rowkey = src + StringUtil.DELIMIT_1ST + uid;
				// fansAnalyz
				status = userFansAnalyzMR.startJob(conf, rowkey, jobPool.name());
				LOG.info("finish userFansAnalyzMR waitForCompletion: " + status);

				// analyzAvg
				wbTweetAnalyzAverage.start(conf, rowkey);
				status = true;
				
				HConnectionManager.deleteConnection(conf, true);
			} else {
				LOG.info("Unrecognized job target uid!");
				status = false;
			}
		} catch (Exception e) {
			e.printStackTrace();
			status = false;
		}
		return status;
	}
}
