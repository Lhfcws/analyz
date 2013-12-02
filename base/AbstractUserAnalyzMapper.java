package com.yeezhao.analyz.base;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.summba.basesupport.domain.TargetUser;
import com.summba.basesupport.domain.TargetUserIndex;
import com.yeezhao.amkt.core.CoreConsts.USER_TYPE;
import com.yeezhao.analyz.group.WeiboUser;
import com.yeezhao.analyz.group.processor.BaseUserProcessor;
import com.yeezhao.analyz.group.processor.ProAgeClassify;
import com.yeezhao.analyz.group.processor.ProGroupLabel;
import com.yeezhao.analyz.group.processor.ProLocationFormat;
import com.yeezhao.analyz.group.processor.ProUserFilter;
import com.yeezhao.analyz.group.processor.ProWeiboFilter;
import com.yeezhao.analyz.group.processor.UserInfoProcessable;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.analyz.util.AppConsts.ANALYZ_OP;
import com.yeezhao.commons.util.Pair;
import com.yeezhao.commons.util.StringUtil;

/**
 * @author yongjiang
 * 
 * @param <KEYIN>
 * @param <VALUEIN>
 */
public abstract class AbstractUserAnalyzMapper<KEYIN, VALUEIN> extends
		Mapper<KEYIN, VALUEIN, Text, Text> {
	protected static Log LOG = LogFactory
			.getLog(AbstractUserAnalyzMapper.class);

	protected static final String CONF_OUTPUT_KEY = "output.dir";
	protected static final String INDEX_DEL = "delete";
	protected static final String INDEX_UPDATE = "update";

	protected static enum MAPPER_COUNTER {
		INVALID, NO_NAME, PREFIX_INVALID
	}

	protected static enum SQL_COUNTER {
		NO_VALUE, EVID_INSERT, INFO_DELETE //, EVID_DELETE
	}

	protected static enum INDEX_COUNTER {
		DELETE, UPDATE
	}

	protected static enum GROUP_COUNTER {
		NOTNULL, NULL
	}

	protected static enum LOCATION_COUNTER {
		NOTNULL, NULL
	}

	protected static enum AGE_COUNTER {
		NOTNULL, NULL
	}

	protected static enum UFIL_COUNTER {
		NOTNULL, NULL
	}

	protected static final String USER_COUNTER_GROUP = "USER_GROUP";
	protected static final String USER_COUNTER_LOCATION = "USER_LOCATION";
	protected static final String USER_COUNTER_AGE = "USER_AGE";
	protected static final String USER_COUNTER_UFIL = "USER_UFIL";

	protected ObjectOutputStream objOS;
	protected OutputStream sqlOS;
	protected String filePrefix = null;
	protected boolean hasOutput = false; // 需要进行输出

	protected Map<AppConsts.ANALYZ_OP, BaseUserProcessor> processorMap;
	protected HTable userProfileTbl;

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		try {
			userProfileTbl = new HTable(conf,
					conf.get(AppConsts.HTBL_USER_PROFILE));
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			processorMap = new HashMap<AppConsts.ANALYZ_OP, BaseUserProcessor>();
			processorMap.put(ANALYZ_OP.GROUP, new ProGroupLabel(conf));
			processorMap.put(ANALYZ_OP.AGE, new ProAgeClassify(conf));
			processorMap.put(ANALYZ_OP.UFIL, new ProUserFilter(conf));
			processorMap.put(ANALYZ_OP.LOC, new ProLocationFormat(conf));
			processorMap.put(ANALYZ_OP.WFIL, new ProWeiboFilter(conf));

			if (conf.get(CONF_OUTPUT_KEY) != null) {
				try {
					filePrefix = context.getTaskAttemptID().toString();
					objOS = new ObjectOutputStream(FileSystem.get(conf).create(
							new Path(new Path(conf.get(CONF_OUTPUT_KEY)),
									filePrefix + AppConsts.OBJ_FILE_SUFFIX)));
					sqlOS = FileSystem.get(conf).create(
							new Path(new Path(conf.get(CONF_OUTPUT_KEY)),
									filePrefix + AppConsts.SQL_USER_EVID_POST
											+ AppConsts.SQL_FILE_SUFFIX));
					hasOutput = true;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			LOG.info("hasOutput: " + hasOutput);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup(Context context) {
		try {
			if (userProfileTbl != null) {
				userProfileTbl.close();
			}
			if (hasOutput) {
				sqlOS.close();
				objOS.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected UserInfoProcessable composeProcessors(String oprCmd) {
		for (BaseUserProcessor processor : processorMap.values())
			// 要清理processor的嵌套关系;
			processor.setInnerProcessor(null);
		String[] oprs = oprCmd.split(StringUtil.STR_DELIMIT_1ST);
		Map<AppConsts.ANALYZ_OP, BaseUserProcessor> oprMap = new HashMap<AppConsts.ANALYZ_OP, BaseUserProcessor>();
		for (String opr : oprs) {
			AppConsts.ANALYZ_OP oprEnum = AppConsts.ANALYZ_OP.valueOf(opr);
			oprMap.put(oprEnum, processorMap.get(oprEnum));
		}
		return nestProcessor(oprMap);
	}

	private UserInfoProcessable nestProcessor(
			Map<AppConsts.ANALYZ_OP, BaseUserProcessor> processors) {
		if (processors.isEmpty())
			return null;
		if (processors.size() == 1) {
			return processors.values().iterator().next();
		}
		Map<AppConsts.ANALYZ_OP, BaseUserProcessor> tmpMap = new TreeMap<AppConsts.ANALYZ_OP, BaseUserProcessor>(
				new Comparator<AppConsts.ANALYZ_OP>() {

					@Override
					public int compare(ANALYZ_OP o1, ANALYZ_OP o2) {
						return o1.getOrder() - o2.getOrder();
					}

				});
		tmpMap.putAll(processors);
		List<AppConsts.ANALYZ_OP> oprs = new LinkedList<AppConsts.ANALYZ_OP>(
				tmpMap.keySet());
		BaseUserProcessor nestProcessor = tmpMap.get(oprs.get(0)); // order为k的作为order为k+1的inner
																	// processor
		for (int i = 1; i < oprs.size(); i++) {
			tmpMap.get(oprs.get(i)).setInnerProcessor(nestProcessor);
			nestProcessor = tmpMap.get(oprs.get(i));
		}
		return nestProcessor;
	}

	/**
	 * analyzer one user
	 * 
	 * @param user
	 * @param analyzOps
	 * @param context
	 */
	protected void analyzOneUser(WeiboUser user, int weiboType,
			String analyzOps, String tblName, Context context) {
		// <basic info processor, footprint processor>
		UserInfoProcessable compositeProcessor = composeProcessors(analyzOps);
		String uid = user.getUid();
		if (user.getSqlProfileMap() == null) {
			context.getCounter(MAPPER_COUNTER.INVALID).increment(1);
			return;
		}
		String nickname = user.getSqlProfileMap().get(
				AppConsts.COL_USR_NICKNAME);
		if (nickname == null || nickname.isEmpty()) {
			LOG.info("uid: " + uid + ", nickname is null");
			context.getCounter(MAPPER_COUNTER.NO_NAME).increment(1);
			return;
		}

		if (compositeProcessor != null) {
			compositeProcessor.processUser(user);
		}
		try {
			Pair<List<Put>, Map<String, String>> resPair = BaseUserProcessor
					.outputUserInfo(user);
			List<Put> tblOprs = resPair.first;
			Map<String, String> sqlProfileMap = resPair.second;
			userProfileTbl.put(tblOprs);

			String[] oprs = analyzOps.split(StringUtil.STR_DELIMIT_1ST);
			boolean deleteUser = false;

			Map<String, String> updtValMap = new HashMap<String, String>(); // 真正更新的<key,value>
			for (String opr : oprs) {
				switch (AppConsts.ANALYZ_OP.valueOf(opr)) { // 对于WFIL操作,没有对应的mysql写入
				case GROUP:
					if (sqlProfileMap.containsKey(AppConsts.COL_USR_GROUP)) {
						String groups = sqlProfileMap
								.get(AppConsts.COL_USR_GROUP);
						updtValMap.put(AppConsts.COL_USR_GROUP, groups);
						// updtValMap.put(AppConsts.COL_USR_EVIDENCE,
						// sqlProfileMap.get(AppConsts.COL_USR_EVIDENCE));
						// 统计标签
						if (groups != null && !groups.isEmpty()) {
							String[] splits = groups
									.split(StringUtil.STR_DELIMIT_1ST); // |分隔
							for (String string : splits)
								if (!string.isEmpty())
									context.getCounter(USER_COUNTER_GROUP,
											string).increment(1);
							context.getCounter(GROUP_COUNTER.NOTNULL)
									.increment(1);
						} else
							context.getCounter(GROUP_COUNTER.NULL).increment(1);
					}
					break;
				case AGE:
					if (sqlProfileMap.containsKey(AppConsts.COL_USR_AGE)) {
						updtValMap.put(AppConsts.COL_USR_AGE,
								sqlProfileMap.get(AppConsts.COL_USR_AGE));
						context.getCounter(USER_COUNTER_AGE,
								sqlProfileMap.get(AppConsts.COL_USR_AGE))
								.increment(1);
						context.getCounter(AGE_COUNTER.NOTNULL).increment(1);
					} else
						context.getCounter(AGE_COUNTER.NULL).increment(1);
					break;
				case UFIL:
					String val = sqlProfileMap.get(AppConsts.COL_USR_TYPE);
					if (val != null) {
						context.getCounter(USER_COUNTER_UFIL, val).increment(1);
						context.getCounter(UFIL_COUNTER.NOTNULL).increment(1);

						if (Integer.parseInt(val) != USER_TYPE.NORMAL.getCode()) {// 非normal用户
							deleteUser = true;
							// LOG.info("uid: " + uid + ", type: " + val);
						}
					} else {
						context.getCounter(UFIL_COUNTER.NULL).increment(1);
						LOG.info("##: " + uid + ", ufil is null");
					}
					break;
				case LOC:
					if (sqlProfileMap
							.containsKey(AppConsts.COL_USR_LOCATIONFORMAT)) {
						String locations = sqlProfileMap
								.get(AppConsts.COL_USR_LOCATIONFORMAT);
						updtValMap.put(AppConsts.COL_USR_LOCATIONFORMAT,
								locations);
						// 统计地区
						if (locations == null || locations.isEmpty())
							context.getCounter(LOCATION_COUNTER.NULL)
									.increment(1);
						else {
							String[] splits = locations
									.split(StringUtil.STR_DELIMIT_3RD); // #分隔
							if (splits.length > 1)
								context.getCounter(USER_COUNTER_LOCATION,
										splits[1]).increment(1);
							else
								context.getCounter(USER_COUNTER_LOCATION,
										locations).increment(1);
							context.getCounter(LOCATION_COUNTER.NOTNULL)
									.increment(1);
						}
					}
					break;
				default:
					break;
				}
				if (deleteUser)
					break;
			}
			if (!hasOutput) // 不需进行转换
				return;

			if (sqlProfileMap.containsKey(AppConsts.COL_USR_TYPE)) // 删除user_type,
																	// 不属于sql
				sqlProfileMap.remove(AppConsts.COL_USR_TYPE);

			String sqlInfoExpr = toSqlOp(uid, tblName, deleteUser,
					updtValMap, context);
			if (sqlInfoExpr != null) // need update
				sqlOS.write(Bytes.toBytes(sqlInfoExpr + ";\n"));
			
			if (objOS != null) {
				TargetUserIndex indexUser = toUserIndex(uid, weiboType,
						deleteUser, sqlProfileMap, context);
				if (indexUser.getOperator() != null) {
					objOS.writeObject(indexUser);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String toSqlOp(String uid, String tblName,
			boolean deleteUser, Map<String, String> updtValMap, Context context) {
		String sqlInfoExpr = null;
		if (deleteUser) {
			sqlInfoExpr = String.format("delete from %s where uid='%s'",
					tblName, uid);
			context.getCounter(SQL_COUNTER.INFO_DELETE).increment(1);
		}
		return sqlInfoExpr;
	}

	/**
	 * 返回更新sql语句
	 * 
	 * @param uid
	 * @param tblName
	 * @param deleteUser
	 * @param needInsert
	 * @param allValMap
	 * @param updtValMap
	 * @param context
	 * @return
	 */
	// public Pair<String, String> toSqlOp( String uid, String tblName, boolean
	// deleteUser, Map<String, String> updtValMap, Context context ) {
	// String sqlInfoExpr = null;
	// String sqlEvidExpr = null;
	// String updtEvids = null;
	//
	// String evidsTblName = tblName + AppConsts.SQL_USER_EVID_POST;
	// if (deleteUser) {
	// sqlInfoExpr = String.format("delete from %s where uid='%s'",
	// tblName, uid);
	// context.getCounter(SQL_COUNTER.INFO_DELETE).increment(1);
	// sqlEvidExpr = String.format("delete from %s where uid='%s'",
	// evidsTblName, uid);
	// context.getCounter(SQL_COUNTER.EVID_DELETE).increment(1);
	// } else {
	// try {
	// updtEvids = updtValMap.remove(AppConsts.COL_USR_EVIDENCE);
	// if (updtEvids != null) {
	// Map<String, String> evidsMap = new HashMap<String, String>();
	// evidsMap.put("uid", uid);
	// evidsMap.put(AppConsts.COL_USR_EVIDENCE, updtEvids);
	// sqlEvidExpr = AppUtil.generateInsertExpr(evidsTblName, evidsMap);
	// if (sqlEvidExpr != null)
	// context.getCounter(SQL_COUNTER.EVID_INSERT).increment(1);
	// }
	// }
	// catch (Exception e) {
	// e.printStackTrace();
	// }
	// }
	// return new Pair<String, String>(sqlInfoExpr, sqlEvidExpr);
	// }

	public TargetUserIndex toUserIndex(String uid, int weiboType,
			boolean deleteUser, Map<String, String> valMap, Context context) {
		// 写入增量索引obj
		TargetUser targetUser = getTargetUser(uid, weiboType, valMap);

		TargetUserIndex indexUser = new TargetUserIndex();
		indexUser.setTargetUser(targetUser);
		if (!deleteUser) {
			indexUser.setOperator(INDEX_UPDATE);
			context.getCounter(INDEX_COUNTER.UPDATE).increment(1);
		} else {
			targetUser = new TargetUser();
			targetUser.setId(uid);
			targetUser.setWeiboType(weiboType);
			indexUser = new TargetUserIndex();
			indexUser.setTargetUser(targetUser);
			indexUser.setOperator(INDEX_DEL);
			context.getCounter(INDEX_COUNTER.DELETE).increment(1);
		}
		return indexUser;
	}

	public TargetUser getTargetUser(String uid, int weiboType,
			Map<String, String> valMap) {
		TargetUser user = new TargetUser();
		user.setId(uid);
		user.setNickName(valMap.get(AppConsts.COL_USR_NICKNAME));
		String gender = valMap.get(AppConsts.COL_USR_GENDER);
		if (gender != null && !gender.isEmpty())
			user.setSex(Integer.parseInt(gender));
		else
			user.setSex(0);
		user.setArea(valMap.get(AppConsts.COL_USR_LOCATIONFORMAT));
		user.setWeiboType(weiboType);
		String age = valMap.get(AppConsts.COL_USR_AGE);
		if (age != null && !age.isEmpty())
			user.setAge(Integer.parseInt(age));
		else
			user.setAge(0);
		String group = valMap.get(AppConsts.COL_USR_GROUP);
		user.setGroup(group == null ? null : group.replace(
				StringUtil.DELIMIT_1ST, StringUtil.DELIMIT_3RD));
		user.setHead(valMap.get(AppConsts.COL_USR_HEAD));
		String followsCount = valMap.get(AppConsts.COL_USR_FOLLOWCOUNT);
		if (followsCount != null && !followsCount.isEmpty())
			user.setFollowersCount(Integer.parseInt(followsCount));
		String verifiedType = valMap.get(AppConsts.COL_USR_VERIFEDTYPE);
		if (verifiedType != null && !verifiedType.isEmpty())
			user.setVerifiedType(Integer.parseInt(verifiedType));
		return user;
	}
}
