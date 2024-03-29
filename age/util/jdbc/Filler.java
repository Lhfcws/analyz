package com.yeezhao.analyz.age.util.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 申明参数绑定方法
 * @author wangkai
 *
 */
public interface Filler {
	/**
	 * 绑定参数
	 * @param psmt
	 * @throws SQLException 
	 */
	public void bind(PreparedStatement psmt) throws SQLException;

}
