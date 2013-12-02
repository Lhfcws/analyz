package com.yeezhao.analyz.age.util.jdbc;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;

public class DataSourceFactoryByConf {

	private DataSource dataSource;
	private Config config;

	public DataSourceFactoryByConf(String dataSourceString) {
		try {
			initConfig(dataSourceString);
			initDataSource();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("数据源初使化失败，请�?��配制文件和数据库");
		} catch (PropertyVetoException e) {
			e.printStackTrace();
			throw new RuntimeException("数据源初使化失败，可能是数据库驱动类的配制问题！");
		}
	}

	private void initConfig(String dataSourceString) throws IOException {
/*		InputStream is = ClassLoader
				.getSystemResourceAsStream("jdbc.properties");*/
		Properties properties = new Properties();
/*		properties.load(is);
		System.out.println(properties.getProperty("jdbcUrl"));*/
		String[] split = dataSourceString.split("[|]");
		if (split.length == 4) {
			properties.setProperty("driverClass", split[0]);
			properties.setProperty("jdbcUrl", split[1]);
			properties.setProperty("user", split[2]);
			properties.setProperty("password", split[3]);
		}
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

	private void initDataSource() throws PropertyVetoException {
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriverClassName(config.getDriverClass());
		dataSource.setUrl(config.getJdbcUrl());
		dataSource.setUsername(config.getUser());
		dataSource.setPassword(config.getPassword());

		this.dataSource = dataSource;
	}

	public DataSource getDataSource() {
		return dataSource;
	}
}
