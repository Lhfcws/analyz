package com.yeezhao.analyz.group;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.yeezhao.commons.util.Pair;
import com.yeezhao.commons.util.StringUtil;

public class KeywordMatchLabeller implements GroupLabel {

	private UserGroup.SrcType dataType;
	protected DataCleanable cleaner;
	private Map<String, List<String>> groupAttrMap;
	private int maxKeywordLen = -1;
	private int minKeywordLen = 100;
	
	public KeywordMatchLabeller(UserGroup.SrcType dataType, Map<String, List<String>> groupAttrMap){
		this.dataType = dataType;
		cleaner = new WeiboContentCleaner();
		this.groupAttrMap = groupAttrMap;
	}
	
	public static void decapsulateKwMap(UserGroup group, Map<String, List<String>> kwMap){
		if(group.getAttributes() == null || group.getAttributes().isEmpty())
			return;
		for(String attr : group.getAttributes()){
			if(!kwMap.containsKey(attr))
				kwMap.put(attr, new LinkedList<String>());
			kwMap.get(attr).add(group.getGroupLabel());
		}
	}
	
	@Override
	public Map<String, List<String>> analyzUserData(Map<String, UserFootPrint> dataMap) {
		Map<String, Set<String>> fp2Labels = new HashMap<String, Set<String>>();
		for(Entry<String, UserFootPrint> item : dataMap.entrySet()){
			if(dataType != UserGroup.SrcType.ALL  //datatype必须满足要求
					&& !dataType.getTypeName().equals(item.getValue().getDataType()))
				continue;
			String newContent = cleaner.cleanFootPrint(item.getValue().getContent());
			newContent = newContent.toUpperCase();
			
			if(maxKeywordLen == -1){
				for(String kw : groupAttrMap.keySet()){
					if(kw.length() > maxKeywordLen)
						maxKeywordLen = kw.length();
					if(kw.length() < minKeywordLen)
						minKeywordLen = kw.length();
				}
			}
			
			List<Pair<Integer, Integer>> poses = StringUtil.backwardMaxMatch(newContent, 
					groupAttrMap, maxKeywordLen, minKeywordLen);
			for(Pair<Integer, Integer> pos : poses){
				String matchWd = newContent.substring(pos.first, pos.second);
				if(!fp2Labels.containsKey(item.getKey()))
					fp2Labels.put(item.getKey(), new HashSet<String>());
				fp2Labels.get(item.getKey()).addAll(groupAttrMap.get(matchWd));
			}
		}
		
		Map<String, List<String>> label2Fp = new HashMap<String, List<String>>();
		for(Entry<String, Set<String>> entry : fp2Labels.entrySet()){
			for(String label : entry.getValue()){
				if(!label2Fp.containsKey(label))
					label2Fp.put(label, new LinkedList<String>());
				label2Fp.get(label).add(entry.getKey());
			}
		}
		return label2Fp;
	}

}
