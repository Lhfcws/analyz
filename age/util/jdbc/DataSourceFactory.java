package com.yeezhao.analyz.age.util.jdbc;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;

public class DataSourceFactory {
	private static DataSource dataSource;
	private static Config config;

	static {

		try {
			initConfig();
			initDataSource();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("数据源初使化失败，请�?��配制文件和数据库");
		} catch (PropertyVetoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException("数据源初使化失败，可能是数据库驱动类的配制问题！");
		}

	}

	private static void initConfig() throws IOException {
		InputStream is = ClassLoader
				.getSystemResourceAsStream("jdbc.properties");
		Properties properties = new Properties();
		properties.load(is);
		System.out.println(properties.getProperty("jdbcUrl"));
		config = new Config();
		String jdbcUrl = properties.getProperty("jdbcUrl");
		String driverClass = properties.getProperty("driverClass");
		String user = properties.getProperty("user");
		String password = properties.getProperty("password");

		if (StringUtils.isBlank(jdbcUrl) || StringUtils.isBlank(driverClass)
				|| StringUtils.isBlank(user) || StringUtils.isBlank(password)) {
			throw new RuntimeException("配制文件不合法，不能为空的字段为");
		}
		config.setDriverClass(driverClass);
		config.setJdbcUrl(jdbcUrl);
		config.setUser(user);
		config.setPassword(password);
	}

	private static void initDataSource() throws PropertyVetoException {
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriverClassName(config.getDriverClass());
		dataSource.setUrl(config.getJdbcUrl());
		dataSource.setUsername(config.getUser());
		dataSource.setPassword(config.getPassword());

		// TODO 处理其他配制项目

		DataSourceFactory.dataSource = dataSource;

	}

	public static DataSource getDataSource() {
		return dataSource;
	}

}
