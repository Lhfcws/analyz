package com.yeezhao.analyz.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.yeezhao.amkt.core.CoreConsts;
import com.yeezhao.commons.util.sql.BaseDao;
import com.yeezhao.commons.util.sql.BaseDaoFactory;

/**
 * 在同一个库执行分表
 * @author yongjiang
 * date 2012-08-12
 */
public class CreateUesrSplitTable {
	Log LOG = LogFactory.getLog(CreateUesrSplitTable.class);
	
	private String snUserTableName = "t_user_info_sina_";
	private String txUserTableName = "t_user_info_tx_";	
	//group_name, uid, evidence, nickname, name, gender, location_format, age
	private String[] columnNames = new String[] {"(`id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',",
			"`uid` varchar(45) DEFAULT NULL COMMENT '微博用户ID',",
			"`name` varchar(45) DEFAULT NULL COMMENT '微博用户名称',",
			"`nickname` varchar(32) DEFAULT NULL COMMENT '微博用户昵称',",
			"`gender` tinyint(4) DEFAULT NULL COMMENT '微博用户性别信息',",
			"`group_name` varchar(300) DEFAULT NULL,",
			"`evidence` text,",
			"`age` int(11) DEFAULT NULL COMMENT '用户年龄段信息',",
			"`location_format` varchar(45) DEFAULT NULL,",
			"PRIMARY KEY (`id`),",
			"KEY `ind_uid_type` (`uid`) USING BTREE",
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='存储用户信息用于广告投放的';"								
	};
	public String[] columnAllNames = new String[] {"(`id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',",
									"`uid` varchar(45) DEFAULT NULL COMMENT '微博用户ID',",
									"`user_id` varchar(45) DEFAULT NULL,",
									"`name` varchar(45) DEFAULT NULL COMMENT '微博用户名称',",
									"`nickname` varchar(32) DEFAULT NULL COMMENT '微博用户昵称',",
									"`location` varchar(32) DEFAULT NULL COMMENT '微博用户地域信息',",
									"`head` varchar(300) DEFAULT NULL COMMENT '微博用户头像地址',",
									"`gender` tinyint(4) DEFAULT NULL COMMENT '微博用户性别信息',",
									"`followers_count` int(11) DEFAULT NULL COMMENT '微博用户粉丝数',",
									"`friends_count` int(11) DEFAULT NULL COMMENT '微博用户关注数',",
									"`statuses_count` int(11) DEFAULT NULL COMMENT '微博用户发表微博数',",
									"`allow_all_act_msg` tinyint(4) DEFAULT NULL COMMENT '是否允许评论',",
									"`verified` tinyint(4) DEFAULT NULL COMMENT '是否认证用户，0：不是认证用户，1：认证用户',",
									"`verified_type` int(11) DEFAULT NULL COMMENT '是否认证类型，1：认证用户，2：企业认证用户',",
									"`verified_reason` varchar(300) DEFAULT NULL COMMENT '认证原因',",
									"`birth_date` varchar(20) DEFAULT NULL,",
									"`classifier_tag` varchar(300) DEFAULT NULL,",
									"`status` varchar(300) DEFAULT NULL,",
									"`group_name` varchar(300) DEFAULT NULL,",
									"`evidence` text,",
									"`age` int(11) DEFAULT NULL COMMENT '用户年龄段信息',",
									"`created_date` datetime DEFAULT NULL,",
									"`last_modified_date` datetime DEFAULT NULL,",
									"`location_format` varchar(45) DEFAULT NULL,",
									"PRIMARY KEY (`id`),",
									"KEY `ind_uid_type` (`uid`) USING BTREE",
									") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='存储用户信息用于广告投放的';"								
	};
	
	private BaseDao baseDao;

	public CreateUesrSplitTable(Configuration conf) throws Exception {
		baseDao = BaseDaoFactory.getDaoBaseInstance( conf.get(AppConsts.MYSQL_WEIBO_URL));
	}
	
	public void deleteOldTable() throws Exception{
		int splitNum = CoreConsts.USER_TABLE_SPLIT_NUM;
		for (int i = 0; i < splitNum; i++) {
			String snTable = snUserTableName + i;
			String txTable = txUserTableName + i;
			String delSql = "drop table " + snTable;
			LOG.info(delSql);
			baseDao.batchExecute(delSql);
		
			delSql = "drop table " + txTable;
			LOG.info(delSql);
			baseDao.batchExecute(delSql);
		}
	}
	
	public void insertSnTable() throws Exception{
		int splitNum = CoreConsts.USER_TABLE_SPLIT_NUM;
		StringBuilder sb = new StringBuilder();
		for (String string : columnNames) {
			sb.append(string);
		}
		String columnSql = sb.toString();
		for (int i = 0; i < splitNum; i++) {
			String insertSql = "CREATE TABLE " + snUserTableName + i + " " + columnSql;
			LOG.info(insertSql);
			baseDao.batchExecute(insertSql);
		}
	}
	
	public void insertTxTable() throws Exception{
		int splitNum = CoreConsts.USER_TABLE_SPLIT_NUM;
		StringBuilder sb = new StringBuilder();
		for (String string : columnNames) {
			sb.append(string);
		}
		String columnSql = sb.toString();
		for (int i = 0; i < splitNum; i++) {
			String insertSql = "CREATE TABLE " + txUserTableName + i + " " + columnSql;
			LOG.info(insertSql);
			baseDao.batchExecute(insertSql);
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// 创建 Options 对象
		Options options = new Options();
		// 添加 -h 参数
		options.addOption("t", true, "type, such as create or delete");
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

		String type = cmd.getOptionValue("t");
		
		System.out.println("type: " + type);
		Configuration conf = AnalyzConfiguration.getInstance();
		CreateUesrSplitTable createUesrSplitTable = new CreateUesrSplitTable(conf);
		if (type.equals("delete"))
				createUesrSplitTable.deleteOldTable();
		else if (type.equals("create")) {
			createUesrSplitTable.insertSnTable();
			createUesrSplitTable.insertTxTable();
		}
	}
}
