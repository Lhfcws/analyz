package com.yeezhao.analyz.util;

import org.apache.hadoop.conf.Configuration;

public class AnalyzConfiguration extends Configuration {
	
	private static final AnalyzConfiguration analyzConfiguration = new AnalyzConfiguration();
	
	private AnalyzConfiguration() {
	    this.addResource( AppConsts.FILE_HBASE_CONFIG );
		this.addResource( AppConsts.FILE_ANALYZER_CONFIG );
		System.out.println("get AnalyzConfiguration: " + Thread.currentThread().getId());
	}
	
	public static AnalyzConfiguration getInstance() {
		return analyzConfiguration;
	}
}
