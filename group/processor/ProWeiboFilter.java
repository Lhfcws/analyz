package com.yeezhao.analyz.group.processor;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import com.yeezhao.analyz.filter.WeiboFilterOp;
import com.yeezhao.analyz.group.UserFootPrint;
import com.yeezhao.analyz.group.WeiboUser;

public class ProWeiboFilter extends BaseUserProcessor {

	private WeiboFilterOp wbFilter;
	
	public ProWeiboFilter(Configuration conf) throws IOException{
		super(conf);
		wbFilter = new WeiboFilterOp(conf);
	}
	
	public ProWeiboFilter(BaseUserProcessor innerProcessor) throws IOException{
		super(innerProcessor);
		wbFilter = new WeiboFilterOp(getProConf());
	}

	@Override
	public WeiboUser processMethod(WeiboUser user) {
		Map<String, WeiboFilterOp.WB_TYPE> filterMap = new HashMap<String, WeiboFilterOp.WB_TYPE>();
		for(Entry<String, UserFootPrint> entry : user.getFootPrints().entrySet()){
			WeiboFilterOp.WB_TYPE wbType = wbFilter.classify(entry.getValue().getContent());
			filterMap.put(entry.getKey(), wbType);
		}
		user.setAnalyz_filtertype(filterMap);
		return user;
	}

	@Override
	public boolean validateParams(WeiboUser user) {
		if (user.getFootPrints().isEmpty())
			return false;
		return true;
	}

}
