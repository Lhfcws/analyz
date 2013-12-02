package com.yeezhao.analyz.group;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.yeezhao.analyz.filter.WeiboFilterOp;
import com.yeezhao.commons.util.StringUtil;

public class WeiboUser {
	private String weiboType;											//"sn"代表新浪, "tx"代表腾讯 
	private String uid;													//用户id
	private String nickname;											//用户昵称
	private String location;											//用户登记地址
	private Map<String, UserFootPrint> footPrints; 						//用户的footprints,格式例如<key, footprint>,key是指
																		//yeezhao.user.profile表的的key值
	
	private String analyz_location;										//地址规整结果
	private String analyz_age;											//用户年龄分析结果
	private String analyz_usertype;										//用户类型分析结果
	private Map<String, WeiboFilterOp.WB_TYPE> analyz_filtertype;						//footprint过滤类型, value等于-1时表示不被过滤
	private List<String> analyz_groups;									//用户归属的人群
	private Map<String, List<UserFootPrint>> analyz_evidences;			//用户归属人群的证据,格式例如<group, List<footprint>>
	private Map<String, String> sqlProfileMap; 						//更新sql依赖字段
	private Map<String, String> usrFilterProfileMap;				//僵尸分析依赖字段
	
	public String getAnalyzResult(){
		StringBuffer sb = new StringBuffer();
		sb.append("[uid=").append(weiboType).append(StringUtil.DELIMIT_1ST).append(uid).append(",");
		sb.append("analyz_location=").append(analyz_location).append(",");
		sb.append("analyz_age=").append(analyz_age).append(",");
		sb.append("analyz_usertype=").append(analyz_usertype).append(",");
		sb.append("analyz_groups=").append(analyz_groups).append(",");
		if(analyz_evidences != null && !analyz_evidences.isEmpty()){
			for(Entry<String, List<UserFootPrint>> entry : analyz_evidences.entrySet()){
				sb.append("\nevd 4 ").append(entry.getKey()).append(":\n");
				for(UserFootPrint fp : entry.getValue())
					sb.append("**").append(fp.getContent()).append("\n");
			}
		}
		sb.append("\n**filter type**\n");
		if(analyz_filtertype != null && !analyz_filtertype.isEmpty()){
			for(Entry<String, WeiboFilterOp.WB_TYPE> entry : analyz_filtertype.entrySet()){
				sb.append(entry.getKey()).append(",").append(entry.getValue()).append("\n");
			}
		}
		sb.append("]");
		return sb.toString();
	}
	
	public String getWeiboType() {
		return weiboType;
	}
	public void setWeiboType(String weiboType) {
		this.weiboType = weiboType;
	}
	public String getUid() {
		return uid;
	}
	public void setUid(String uid) {
		this.uid = uid;
	}
	public String getNickname() {
		return nickname;
	}
	public void setNickname(String nickname) {
		this.nickname = nickname;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public Map<String, UserFootPrint> getFootPrints() {
		return footPrints;
	}
	public void setFootPrints(Map<String, UserFootPrint> footPrints) {
		this.footPrints = footPrints;
	}
	public String getAnalyz_location() {
		return analyz_location;
	}
	public void setAnalyz_location(String analyz_location) {
		this.analyz_location = analyz_location;
	}
	public String getAnalyz_age() {
		return analyz_age;
	}
	public void setAnalyz_age(String analyz_age) {
		this.analyz_age = analyz_age;
	}
	public String getAnalyz_usertype() {
		return analyz_usertype;
	}
	public void setAnalyz_usertype(String analyz_usertype) {
		this.analyz_usertype = analyz_usertype;
	}
	public Map<String, WeiboFilterOp.WB_TYPE> getAnalyz_filtertype() {
		return analyz_filtertype;
	}
	public void setAnalyz_filtertype(Map<String, WeiboFilterOp.WB_TYPE> analyz_filtertype) {
		this.analyz_filtertype = analyz_filtertype;
	}
	public List<String> getAnalyz_groups() {
		return analyz_groups;
	}
	public void setAnalyz_groups(List<String> analyz_groups) {
		this.analyz_groups = analyz_groups;
	}
	public Map<String, List<UserFootPrint>> getAnalyz_evidences() {
		return analyz_evidences;
	}
	public void setAnalyz_evidences(
			Map<String, List<UserFootPrint>> analyz_evidences) {
		this.analyz_evidences = analyz_evidences;
	}
	
	public Map<String, String> getSqlProfileMap() {
		return sqlProfileMap;
	}

	public void setSqlProfileMap(Map<String, String> sqlProfileMap) {
		this.sqlProfileMap = sqlProfileMap;
	}

	public Map<String, String> getUsrFilterProfileMap() {
		return usrFilterProfileMap;
	}

	public void setUsrFilterProfileMap(Map<String, String> usrFilterProfileMap) {
		this.usrFilterProfileMap = usrFilterProfileMap;
	}
}
