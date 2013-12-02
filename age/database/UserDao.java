package com.yeezhao.analyz.age.database;

import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.yeezhao.analyz.age.util.jdbc.DaoBase;
import com.yeezhao.analyz.age.util.jdbc.DataSourceFactoryByConf;
import com.yeezhao.analyz.util.AppConsts;

public class UserDao extends DaoBase {

	private final Logger logger = Logger.getLogger(UserDao.class);

	public UserDao(String dataSourceString) {
		setDataSource(new DataSourceFactoryByConf(dataSourceString)
				.getDataSource());
	}

	public void updateUserAge(int sourceType, String userId, int age)
			throws SQLException {

		String tableName = sourceType == 1 ? AppConsts.SQL_USER_SINA
				: AppConsts.SQL_USER_TX;
		String sql = String
				.format("UPDATE %s SET " + " age = %d " + " WHERE 1 "
						+ " AND uid = '%s'" + ";", tableName, age, userId);
		int affectedRows = this.update(sql);

		logger.info("Affected rows: " + affectedRows);
		logger.info("select sql is : " + sql);
	}

}
