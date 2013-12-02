package com.yeezhao.analyz.duanalyz;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.yeezhao.amkt.dujob.DUConsts.AnalyzType;
import com.yeezhao.amkt.dujob.DUConsts;
import com.yeezhao.amkt.dujob.DUJob;
import com.yeezhao.amkt.dujob.DUTaskZkWatcher;
import com.yeezhao.amkt.dujob.DUJob.JobStatus;
import com.yeezhao.commons.zookeeper.ZooKeeperFacade;

/**
 * ZK Watcher class for GEN tasks.
 * @author yongjiang
 */
public class DUAnalyzTaskZkWatcher extends DUTaskZkWatcher{
	private static final Log LOG = LogFactory.getLog( DUAnalyzTaskZkWatcher.class );
	
	public DUAnalyzTaskZkWatcher( ZooKeeperFacade zkf, int threadNum ){
		super(zkf);
		initTaskRunner( threadNum );
	}
	
	private void initTaskRunner(int threadNum){
		for (int i = 0; i < threadNum; i++)
			new DuFansAnalyzRunner(this).start();
		for (int i = 0; i < 2; i++)
			new DuUserAnalyzRunner(this).start();
	}
	
	@Override
	public boolean addJob(DUJob duJob) {
		AnalyzType analyzType = duJob.getAnalyzType();
		LOG.info("addJob: " + duJob.getJobType().name() + ", analyzType: " + analyzType + ", uid: " + duJob.get(DUConsts.PARAM_TARGET_UID) + ", opLog: " + duJob.get(DUConsts.PARAM_OPLOG_PATH));
		System.out.println("addJob: " + duJob.getJobType().name() + ", analyzType: " + analyzType + ", uid: " + duJob.get(DUConsts.PARAM_TARGET_UID) + ", opLog: " + duJob.get(DUConsts.PARAM_OPLOG_PATH));
	
		switch (analyzType) {
			case FANS:
			case USER:
				String taskType = analyzType.name();
				ConcurrentLinkedQueue<DUJob> jobQueue = taskType2JobQueue.containsKey(taskType)? 
						taskType2JobQueue.get(taskType): new ConcurrentLinkedQueue<DUJob>();
				jobQueue.add(duJob);
				LOG.info("taskType: " + taskType + ", queue size: " + jobQueue.size());
				taskType2JobQueue.put(taskType, jobQueue);
				return true;
				
				default:
					LOG.error("Unsupported analyz type: " + analyzType);
					duJob.setJobStatus(JobStatus.FAIL);	
		}
		return false;
	}
}
