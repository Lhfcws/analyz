package com.yeezhao.analyz.group.processor;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

import com.yeezhao.analyz.filter.WeiboFilterOp;
import com.yeezhao.analyz.group.GroupLabeller;
import com.yeezhao.analyz.group.UserFootPrint;
import com.yeezhao.analyz.group.WeiboUser;

public class ProGroupLabel extends BaseUserProcessor {

	private GroupLabeller grpLabeller;
	
	public ProGroupLabel(Configuration conf) throws IOException{
		super(conf);
		grpLabeller = new GroupLabeller(conf);
	}
	
	public ProGroupLabel(BaseUserProcessor innerProcessor) throws IOException{
		super(innerProcessor);
		grpLabeller = new GroupLabeller(getProConf());
	}
	
	@Override
	public WeiboUser processMethod(WeiboUser user) {
		Map<String, UserFootPrint> fpMap = new HashMap<String, UserFootPrint>();
		for(Entry<String, UserFootPrint> entry : user.getFootPrints().entrySet()){
			if(user.getAnalyz_filtertype() != null
					&& user.getAnalyz_filtertype().get(entry.getKey()) != WeiboFilterOp.WB_TYPE.NORMAL){
				continue;
			}
			fpMap.put(entry.getKey(), entry.getValue());
		}
		Map<String, List<String>> groupMap = grpLabeller.labelFootPrints(fpMap);
		if(groupMap != null && !groupMap.isEmpty()){
			user.setAnalyz_groups(new LinkedList<String>(groupMap.keySet()));
			Map<String, List<UserFootPrint>> evdMap = new HashMap<String, List<UserFootPrint>>();
			for(Entry<String, List<String>> entry : groupMap.entrySet()){
				evdMap.put(entry.getKey(), new LinkedList<UserFootPrint>());
				for(String ufpkey : entry.getValue()){
					evdMap.get(entry.getKey()).add(user.getFootPrints().get(ufpkey));
				}
			}
			user.setAnalyz_evidences(evdMap);
		}
		return user;
	}

	@Override
	public boolean validateParams(WeiboUser user) {
		if (user.getFootPrints().isEmpty())
			return false;
		return true;
	}
	
}
