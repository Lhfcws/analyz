package com.yeezhao.analyz.group.processor;


import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.yeezhao.analyz.group.UserFootPrint;
import com.yeezhao.analyz.group.WeiboUser;
import com.yeezhao.analyz.filter.UserFilterOp;
import com.yeezhao.commons.classifier.base.Document;

public class ProUserFilter extends BaseUserProcessor {
	Log LOG = LogFactory.getLog(ProUserFilter.class);
	
	private UserFilterOp userFilter;
	
	public ProUserFilter(Configuration conf) throws IOException{
		super(conf);
		userFilter = new UserFilterOp(conf);
	}
	
	public ProUserFilter(BaseUserProcessor innerProcessor) throws IOException{
		super(innerProcessor);
		userFilter = new UserFilterOp(innerProcessor.getProConf());
	}
	
	@Override
	public WeiboUser processMethod(WeiboUser user) {
		Document doc = new Document(user.getUid());
		Map<String, String> usrFilterProfileMap = user.getUsrFilterProfileMap();
		Set<Entry<String, String>> entries = usrFilterProfileMap.entrySet();
		for ( Entry<String, String> entry : entries) {
			doc.putField(entry.getKey(), entry.getValue());
		}
		
		List<String> tweets = new LinkedList<String>();
		List<Long> time = new LinkedList<Long>();
		if (user.getFootPrints() != null)
			for(Entry<String, UserFootPrint> entry : user.getFootPrints().entrySet()){
				tweets.add(entry.getValue().getContent());
				try {
					time.add( Long.parseLong(entry.getValue().getPublishTime()));
				}
				catch (Exception e) { e.printStackTrace(); }
			}
		user.setAnalyz_usertype( userFilter.classify(doc, tweets, time) );
		return user;
	}

	@Override
	public boolean validateParams(WeiboUser user) {
		if (user.getNickname() == null || user.getNickname().isEmpty())
			return false;
		return true;
	}

}
