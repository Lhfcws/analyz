package com.yeezhao.analyz.group;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.yeezhao.analyz.util.AppConsts;

public class UserGroup implements GroupLabel {
	
	private GroupLabel labeller;
	public GroupLabel getLabeller() {
		return labeller;
	}
	public void setLabeller(GroupLabel labeller) {
		this.labeller = labeller;
	}
	
	private String groupLabel;
	public String getGroupLabel() {
		return groupLabel;
	}
	private Set<String> attributes;
	
	public Set<String> getAttributes() {
		return attributes;
	}
	public void setAttributes(Set<String> attributes) {
		this.attributes = attributes;
	}
	public static enum SrcType{
		ALL("all"),
		MSG(AppConsts.FP_MSG),
		TAG(AppConsts.FP_TAG),
		FOLLOW("follow");
		private String typeName;
		SrcType(String typeName){
			this.typeName = typeName;
		}
		public String getTypeName(){
			return typeName;
		}
	}
	private SrcType analyzDataType;
	
	public SrcType getAnalyzDataType() {
		return analyzDataType;
	}
	public static enum GroupType{
		HOBBY, OCCUPATION, PERSONALITY, INDUSTRY, STATUS, FANS;
	}
	private GroupType groupType;
	
	
	public GroupType getGroupType() {
		return groupType;
	}
	public UserGroup(String groupLabel, SrcType dataType, GroupType groupType){
		this.groupLabel = groupLabel;
		this.analyzDataType = dataType;
		this.groupType = groupType;
	}
	@Override
	public Map<String, List<String>> analyzUserData(
			Map<String, UserFootPrint> dataMap) {
		return labeller == null ? null : labeller.analyzUserData(dataMap);
	}
}
