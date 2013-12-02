package com.yeezhao.analyz.group;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.sjb.ontology.FileHandler;
import com.yeezhao.analyz.group.UserGroup.GroupType;
import com.yeezhao.analyz.group.UserGroup.SrcType;
import com.yeezhao.commons.util.StringUtil;

public class GroupLabelerCache {
	//Specify valid src/group type processed in this labeller
	private static EnumSet<SrcType> VALID_SRCTYPE = 
			EnumSet.of(SrcType.ALL, SrcType.TAG, SrcType.MSG );
	private static EnumSet<GroupType> VALID_GRPTYPE = 
			EnumSet.of(GroupType.HOBBY, GroupType.INDUSTRY, 
					GroupType.OCCUPATION, GroupType.PERSONALITY, GroupType.STATUS);
	
	private Map<String, UserGroup> groupMap = new HashMap<String, UserGroup>();
	private static final String MODEL_BLOCK_START_LINE = "@MODEL";
	
	/**
	 * 获取所有GroupLabeler的接口.
	 * @return
	 */
	public List<GroupLabel> listUserGroups(){
		List<GroupLabel> labelers = new LinkedList<GroupLabel>();
		Map<String, List<String>> kwTagMap = new HashMap<String, List<String>>();
		Map<String, List<String>> kwMsgMap = new HashMap<String, List<String>>();
		
		for(Entry<String, UserGroup> entry : groupMap.entrySet()){
			switch(entry.getValue().getAnalyzDataType()){
			case ALL:
				KeywordMatchLabeller.decapsulateKwMap(entry.getValue(), kwTagMap);
				KeywordMatchLabeller.decapsulateKwMap(entry.getValue(), kwMsgMap);
				break;
			case TAG:
				KeywordMatchLabeller.decapsulateKwMap(entry.getValue(), kwTagMap);
				break;
			case MSG:
				KeywordMatchLabeller.decapsulateKwMap(entry.getValue(), kwMsgMap);
				break;
			default:
				break;
			}
		}
		
		labelers.add(new KeywordMatchLabeller(UserGroup.SrcType.MSG, kwMsgMap));
		labelers.add(new KeywordMatchLabeller(UserGroup.SrcType.TAG, kwTagMap));
		return labelers;
	}
	
	private void addGroup(UserGroup userGroup){
		groupMap.put(userGroup.getGroupLabel(), userGroup);
	}
	
	/**
	 * 根据groupName获取对应的UserGroup对象。
	 * @param groupLabel
	 * @return
	 */
	public UserGroup getUserGroup(String groupLabel){
		return groupMap.get(groupLabel);
	}
	
	public GroupLabelerCache(InputStream usrModelFile) throws IOException{
		initCacheFromConfig(usrModelFile);
	}
	
	private void initCacheFromConfig(InputStream modelFile) throws IOException{
		List<String> lines = FileHandler.readKnowlegeFileLines(modelFile);
		List<String> modelLines = new LinkedList<String>();
		boolean readModel = false;
		for(String line : lines){
			line = line.trim();
			if(readModel){
				if(!line.isEmpty())
					modelLines.add(line);
			} else if(line.equals(MODEL_BLOCK_START_LINE)){
				readModel = true;
			} else {
				if(!line.isEmpty()){
					String[] groupDefs = line.split(StringUtil.STR_DELIMIT_1ST);
					if( groupDefs.length != 3 ){
						System.err.println("Invalid line: " + line );
						continue;
					}

					SrcType srcType = SrcType.valueOf(groupDefs[1]); 
					GroupType groupType = GroupType.valueOf(groupDefs[2]);

					if( VALID_SRCTYPE.contains(srcType) 
							&& VALID_GRPTYPE.contains(groupType) ){
						UserGroup group = new UserGroup(groupDefs[0], srcType, groupType);
						addGroup(group);
					}
				}
			}
		}
		for(String line : modelLines){
			String[] segs = line.split(StringUtil.STR_DELIMIT_1ST);
			if( groupMap.containsKey(segs[0]))
				getUserGroup(segs[0]).setAttributes(new HashSet<String>(
					Arrays.asList(segs[1].split(StringUtil.STR_DELIMIT_2ND))));
		}
	}
}
