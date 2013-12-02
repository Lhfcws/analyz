package com.yeezhao.analyz.base;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;


public interface UserAnalyzRunner {
	public static final String CONF_OUTPUT_KEY = "output.dir";
	public static final String CLI_PARAM_NEEDSPLIT = "needSplit";
	public static final String CLI_PARAM_SRC = "src";
	public static final String CLI_PARAM_ONLYDEL = "onlyDel";
	
	/**
	 * setup the job
	 * @param job
	 */
	public void initMapperJob( Job job, Scan scan) throws IOException;
}
