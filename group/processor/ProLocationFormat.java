package com.yeezhao.analyz.group.processor;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.sjb.ontology.OntoUtil;
import com.sjb.ontology.tpclassifier.TreepathClassifierFactory;
import com.yeezhao.analyz.group.WeiboUser;
import com.yeezhao.analyz.util.AppConsts;

public class ProLocationFormat extends BaseUserProcessor {
	private TreepathClassifierFactory classifier;
	
	public ProLocationFormat(Configuration conf){
		super(conf);
		String knowPath = conf.getResource(conf.get("konwledge.dir")).toString();
		knowPath = knowPath.substring(knowPath.indexOf(":") + 1)
				+ File.separatorChar;
		classifier = new TreepathClassifierFactory(knowPath);
	}
	
	public ProLocationFormat(BaseUserProcessor innerProcessor){
		super(innerProcessor);
		String knowPath = getProConf().getResource(getProConf().get("konwledge.dir")).toString();
		knowPath = knowPath.substring(knowPath.indexOf(":") + 1)
				+ File.separatorChar;
		classifier = new TreepathClassifierFactory(knowPath);
	}
	
	@Override
	public WeiboUser processMethod(WeiboUser user) {
		if(user.getLocation() == null)
			return user;
		Map<String, String> locMap = new HashMap<String, String>();
		locMap.put(OntoUtil.WEIBO_TPCLS_ATTRIBUTES.TEXT, user.getLocation());
		List<String> paths = classifier.getCandidateTreepath(1001, locMap);
		user.setAnalyz_location(paths.isEmpty() ? AppConsts.LOC_FORMAT_DEFAULT : paths.get(0));
		return user;
	}

	@Override
	public boolean validateParams(WeiboUser user) {
		if (user.getLocation() == null)
			return false;
		return true;
	}

}
