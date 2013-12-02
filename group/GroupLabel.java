package com.yeezhao.analyz.group;

import java.util.List;
import java.util.Map;

public interface GroupLabel {
	/**
	 * 分析一个用户的所有footprint,分析用户可能属于的group.
	 * @param dataMap
	 * @return <groupName, list of footprint number>
	 */
	public Map<String, List<String>> analyzUserData(Map<String, UserFootPrint> dataMap);
}
