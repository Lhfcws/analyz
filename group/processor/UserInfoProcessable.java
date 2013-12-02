package com.yeezhao.analyz.group.processor;

import com.yeezhao.analyz.group.WeiboUser;

public interface UserInfoProcessable {
	
	/**
	 * validate related params
	 * @param user
	 * @return
	 */
	public boolean validateParams(WeiboUser user);
	
	/**
	 * 
	 * @param user
	 * @return
	 */
	public WeiboUser processUser(WeiboUser user);
}
