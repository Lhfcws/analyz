package com.yeezhao.analyz.duanalyz;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.yeezhao.amkt.dujob.AbstractDUJobQueue;
import com.yeezhao.amkt.dujob.AbstractDURunner;
import com.yeezhao.amkt.dujob.DUConsts;
import com.yeezhao.amkt.dujob.DUConsts.AnalyzType;
import com.yeezhao.amkt.dujob.DUJob;
import com.yeezhao.analyz.util.AnalyzConfiguration;

/**
 * 
 * @author yongjiang
 * 
 */
public abstract class AbstractAnalyzDURunner extends AbstractDURunner {
	Log LOG = LogFactory.getLog(AbstractAnalyzDURunner.class);
	protected final Configuration conf;

	public AbstractAnalyzDURunner(AbstractDUJobQueue jobQueue) {
		super(jobQueue);
		conf = AnalyzConfiguration.getInstance();
	}

	// Parameters not allowed to be null
	private static String[] fansRequiredParamList = new String[] {
			DUConsts.PARAM_TARGET_UID,
			DUConsts.PARAM_NICKNAME, DUConsts.PARAM_SRC_ID };

	private static String[] userRequiredParamList = new String[] { };
	
	private static String[] defaultRequiredParamList = new String[]{};

	@Override
	protected boolean validateJobParams(DUJob duJob) {
		if (!super.validateJobParams(duJob))
			return false;

		AnalyzType analyzType = duJob.getAnalyzType();
		if (analyzType == AnalyzType.FANS) {
			try {
				int srcID = Integer.parseInt(duJob.get(DUConsts.PARAM_SRC_ID));
				if (srcID < 1 || srcID > 4) {
					System.err.println("Invalid src id: "
							+ duJob.get(DUConsts.PARAM_SRC_ID));
					return false;
				}			
			} catch (NumberFormatException e) {
				System.err.println("Invalid src id:"
						+ duJob.get(DUConsts.PARAM_SRC_ID));
				return false;
			}
		}
		else if (analyzType == AnalyzType.USER) {
			if( !duJob.containsKey(DUConsts.PARAM_OPLOG_PATH) && !duJob.containsKey(DUConsts.PARAM_ANALYZ_OP) ) { 
				System.err.println("not contain oplogPath or analyzOP!");
				return false;
			}
		}
		
		return true;
	}

	@Override
	public String[] getRequiredParamList(DUJob duJob) {
		AnalyzType analyzType = duJob.getAnalyzType();
		if (analyzType == AnalyzType.FANS)
			return fansRequiredParamList;
		else if (analyzType == AnalyzType.USER)
			return userRequiredParamList;
		return defaultRequiredParamList;
	}
}
