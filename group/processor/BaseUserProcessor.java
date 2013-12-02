package com.yeezhao.analyz.group.processor;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.yeezhao.analyz.filter.WeiboFilterOp;
import com.yeezhao.analyz.group.UserFootPrint;
import com.yeezhao.analyz.group.WeiboUser;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.analyz.util.AppUtil;
import com.yeezhao.commons.hbase.HBaseUtil;
import com.yeezhao.commons.util.Pair;
import com.yeezhao.commons.util.StringUtil;

abstract public class BaseUserProcessor implements UserInfoProcessable {
	
	private BaseUserProcessor innerProcessor;
	private Configuration proConf;
	
	public void setInnerProcessor(BaseUserProcessor innerProcessor){
		this.innerProcessor = innerProcessor;
	}
	
	public BaseUserProcessor(Configuration conf){
		this.proConf = conf;
	};
	
	public BaseUserProcessor(BaseUserProcessor innerProcessor){
		this.innerProcessor = innerProcessor;
		this.proConf = innerProcessor.getProConf();
	}
	
	public Configuration getProConf(){
		return proConf;
	}
	
	@Override
	public WeiboUser processUser(WeiboUser user) {
		return processMethod(
				innerProcessor == null ? user : innerProcessor.processUser(user));
	}
	
	abstract public WeiboUser processMethod(WeiboUser user);
	
//	public BaseUserProcessor getInnerProcessor(){
//		return innerProcessor;
//	}
	
	public static Result readBasicInfo(HTable userInfoTbl, WeiboUser user) throws IOException{
		String idprefix = new StringBuilder().append(user.getWeiboType()).append(StringUtil.DELIMIT_1ST).
				append(user.getUid()).toString();
		Get get = new Get(Bytes.toBytes(idprefix));
		get.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_NICKNAME);
		get.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_LOCATION);
		get.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_GENDER);	
		get.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_HEAD);
		get.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_VERIFIEDTYPE);
		
		get.addColumn( AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_AGE);
		get.addColumn( AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_LOCATION_FORMAT);
		get.addColumn( AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_USRTYPE);
		get.addColumn( AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_USRGROUP);
		//僵尸分析依赖字段
		get.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_FRIENDCOUNT);
		get.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_FOLLOWCOUNT);
		get.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_STATUSCOUNT);
		get.addColumn( AppConsts.COL_FMLY_CRAWL, AppConsts.COL_TWEETNUM);
//		if (readEvidence)
//			get.addFamily( AppConsts.COL_FMLY_EVIDENCE);
		Result row = userInfoTbl.get( get);
		return row;
	}
	
	/**
	 * 根据用户id和用户数据源名称,从footprint表中读取该用户的所有footprint。
	 * @param fpTbl
	 * @param user
	 * @return
	 * @throws IOException
	 */
	public static HashMap<String, UserFootPrint> readFootprints(HTable profileTbl, WeiboUser user) throws IOException{
		String idprefix = new StringBuilder().append(user.getWeiboType()).append(StringUtil.DELIMIT_1ST).
				append(user.getUid()).append(StringUtil.DELIMIT_1ST).toString();
		Scan scan = new Scan();
		scan.addFamily( AppConsts.COL_FMLY_FP);
		scan.setStartRow(Bytes.toBytes(idprefix));
		scan.setStopRow(Bytes.toBytes(HBaseUtil.getPrefixUpperBound(idprefix)));
		ResultScanner scanner = profileTbl.getScanner(scan);
		Result rs = null;
		HashMap<String, UserFootPrint> fpMap = new HashMap<String, UserFootPrint>();
		while((rs = scanner.next()) != null){
			String key = Bytes.toString(rs.getRow());
			String[] segs = AppUtil.splitFpRowKey(key, StringUtil.STR_DELIMIT_1ST);
			String pbTime = AppUtil.getColumnValue(rs, AppConsts.COL_FMLY_FP, AppConsts.COL_TIME);
			String content = AppUtil.getColumnValue(rs, AppConsts.COL_FMLY_FP, AppConsts.COL_CONTENT);
			UserFootPrint fp = new UserFootPrint(segs[2], pbTime, content, segs[3]);
			fpMap.put(key, fp);
		}
		scanner.close();
		return fpMap;
	}
	
	/**
	 * 设置
	 * @param user
	 * @param rs
	 */
	public static WeiboUser setBasicUser(WeiboUser user, Result rs) {
		Map<String, String> sqlProfileMap = new HashMap<String, String>();
		Map<String, String> usrFilterProfileMap = new HashMap<String, String>();
		
		String nickName = AppUtil.getColumnValue(rs, AppConsts.COL_FMLY_CRAWL, AppConsts.COL_NICKNAME); 
		user.setNickname( nickName );
		user.setLocation( AppUtil.getColumnValue(rs, AppConsts.COL_FMLY_CRAWL, AppConsts.COL_LOCATION));	
		sqlProfileMap.put( AppConsts.COL_USR_NICKNAME, nickName);
		
		String gender = AppUtil.getColumnValue(rs, AppConsts.COL_FMLY_CRAWL, AppConsts.COL_GENDER);
		if (gender != null)
			sqlProfileMap.put( AppConsts.COL_USR_GENDER, gender);
		String head = AppUtil.getColumnValue(rs, AppConsts.COL_FMLY_CRAWL, AppConsts.COL_HEAD);
		if (head != null)
			sqlProfileMap.put( AppConsts.COL_USR_HEAD, head);
		String followsCount = AppUtil.getColumnValue(rs, AppConsts.COL_FMLY_CRAWL, AppConsts.COL_FOLLOWCOUNT);
		if (followsCount != null)
			sqlProfileMap.put( AppConsts.COL_USR_FOLLOWCOUNT, followsCount);
		String verifiedType = AppUtil.getColumnValue(rs, AppConsts.COL_FMLY_CRAWL, AppConsts.COL_VERIFIEDTYPE);
		if (verifiedType != null)
			sqlProfileMap.put( AppConsts.COL_USR_VERIFEDTYPE, verifiedType);
		
		//读取analyz字段
		String age = AppUtil.getColumnValue(rs, AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_AGE);
		if (age != null)
			sqlProfileMap.put( AppConsts.COL_USR_AGE , age);
		String locationFormat = AppUtil.getColumnValue(rs, AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_LOCATION_FORMAT);
		if (locationFormat != null)
			sqlProfileMap.put( AppConsts.COL_USR_LOCATIONFORMAT, locationFormat);
		String usrType = AppUtil.getColumnValue(rs, AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_USRTYPE);
		if (usrType != null)
			sqlProfileMap.put( AppConsts.COL_USR_TYPE, usrType);
		String groupName = AppUtil.getColumnValue(rs, AppConsts.COL_FMLY_ANALYZ, AppConsts.COL_USRGROUP);
		if (groupName != null ) 
			sqlProfileMap.put( AppConsts.COL_USR_GROUP, groupName);
		
		//设置filter字段
		usrFilterProfileMap.put( AppConsts.COL_USR_NICKNAME, nickName);
		String friendCount = AppUtil.getColumnValue( rs, AppConsts.COL_FMLY_CRAWL, AppConsts.COL_FRIENDCOUNT);
		if (friendCount != null)
			usrFilterProfileMap.put( AppConsts.COL_USR_FRIENDCOUNT, friendCount);
		String followCount = AppUtil.getColumnValue( rs,  AppConsts.COL_FMLY_CRAWL, AppConsts.COL_FOLLOWCOUNT);
		if (followCount != null)
			usrFilterProfileMap.put( AppConsts.COL_USR_FOLLOWCOUNT, followCount);
		String statusCount = AppUtil.getColumnValue( rs,  AppConsts.COL_FMLY_CRAWL, AppConsts.COL_STATUSCOUNT);
		if (statusCount != null)
			usrFilterProfileMap.put( AppConsts.COL_USR_STATUSCOUNT, statusCount);
		String tweetNum = AppUtil.getColumnValue( rs,  AppConsts.COL_FMLY_CRAWL, AppConsts.COL_TWEETNUM);
		if (tweetNum != null)
			usrFilterProfileMap.put( AppConsts.COL_USR_TWEETNUM, tweetNum);
		
		user.setSqlProfileMap(sqlProfileMap);
		user.setUsrFilterProfileMap(usrFilterProfileMap);
		return user;
	}
	
//	/**
//	 * 设置evidence
//	 * @param user
//	 * @param rs
//	 * @return
//	 */
//	public static WeiboUser setUserEvidence( WeiboUser user, Result rs) {
//		Map<String, String> sqlProfileMap = user.getSqlProfileMap();
//		
//		Map<byte[], byte[]> colMap = rs.getFamilyMap( AppConsts.COL_FMLY_EVIDENCE);
//		if(colMap != null && !colMap.isEmpty()){
//			StringBuilder sb = new StringBuilder();
//			//evidence format: IT#1335831259#wb#111641109333681$content|...
//			HashSet<String> containGroupSet = new HashSet<String>();
//			for(Entry<byte[], byte[]> evdEntry : colMap.entrySet()){
//				String evdprefix = Bytes.toString(evdEntry.getKey()).trim();
//				String[] splits = evdprefix.split(StringUtil.STR_DELIMIT_1ST);
//				if (!containGroupSet.contains(splits[0])) {
//					sb.append(evdprefix.replaceAll(StringUtil.STR_DELIMIT_1ST, 
//							""+StringUtil.DELIMIT_3RD)).append(StringUtil.DELIMIT_2ND);
//					String evdContent = Bytes.toString(evdEntry.getValue());
//					evdContent = evdContent.replaceAll("[$#]", " ");
//					evdContent = evdContent.replace("|", "/");
//					evdContent = evdContent.replaceAll("(\r|\n)", "");
//					sb.append(evdContent).append(StringUtil.DELIMIT_1ST);
//					
//					containGroupSet.add(splits[0]);
//				}
//			}
//			sqlProfileMap.put(AppConsts.COL_USR_EVIDENCE, AppUtil.stripSpecialSqlChar(sb.toString()));
//		}
//		user.setSqlProfileMap(sqlProfileMap);
//		return user;
//	}
	
	
	/**
	 * 根据用户id和用户数据源名称,读取该用户的所有footprint。
	 * 返回weiboUser对象，以及旧的analyz字段
	 * @param fpTbl
	 * @param user
	 * @return
	 * @throws IOException
	 */
	public static WeiboUser readProfileInfo(HTable profileTbl, WeiboUser user) throws IOException{
		//uid的所有数据不一定连续存储，按字典序排序
		Result rs = readBasicInfo(profileTbl, user);
		if (rs != null) {
			user = setBasicUser(user, rs);
//			if (readEvidence)
//				user = setUserEvidence(user, rs);
		}
		
		Scan scan = new Scan();
		scan.addColumn(AppConsts.COL_FMLY_FP, AppConsts.COL_TIME);
		scan.addColumn(AppConsts.COL_FMLY_FP, AppConsts.COL_CONTENT);

		String idprefix = new StringBuilder().append(user.getWeiboType()).append(StringUtil.DELIMIT_1ST).
				append(user.getUid()).append(StringUtil.DELIMIT_1ST).toString();
		
		scan.setStartRow(Bytes.toBytes(idprefix));
		scan.setStopRow(Bytes.toBytes(HBaseUtil.getPrefixUpperBound(idprefix)));
		ResultScanner scanner = profileTbl.getScanner(scan);
		Map<String, UserFootPrint> fpMap = new HashMap<String, UserFootPrint>();
		while((rs = scanner.next()) != null){
			String key = Bytes.toString(rs.getRow());		
			String[] segs = AppUtil.splitFpRowKey(key, StringUtil.STR_DELIMIT_1ST);
			if (segs.length > 2) {
				String pbTime = AppUtil.getColumnValue(rs, AppConsts.COL_FMLY_FP, AppConsts.COL_TIME); 
				String content = AppUtil.getColumnValue(rs, AppConsts.COL_FMLY_FP, AppConsts.COL_CONTENT);
				UserFootPrint fp = new UserFootPrint(segs[2], pbTime, content, segs[3]);
				fpMap.put(key, fp);
			}
		}
		scanner.close();
		user.setFootPrints(fpMap);
		return user;
	}
	
	/**
	 * 将weibo user信息的分析结果封装成hbase的put操作。
	 * @param user
	 * @return <list of puts>
	 * @throws IOException 
	 */
	public static Pair<List<Put>, Map<String, String>> outputUserInfo(WeiboUser user) throws IOException{
		Map<String, String> valMap = new HashMap<String, String>();
		Map<String, String> sqlProfileMap = user.getSqlProfileMap();
		if(user.getAnalyz_usertype() != null){
			valMap.put( AppConsts.COL_USR_TYPE, user.getAnalyz_usertype());
			sqlProfileMap.put( AppConsts.COL_USR_TYPE, user.getAnalyz_usertype());
		}
		if(user.getAnalyz_location() != null){
			valMap.put( AppConsts.COL_USR_LOCATIONFORMAT, user.getAnalyz_location());
			sqlProfileMap.put( AppConsts.COL_USR_LOCATIONFORMAT, user.getAnalyz_location());
		}
		if(user.getAnalyz_age() != null){
			valMap.put( AppConsts.COL_USR_AGE, user.getAnalyz_age());
			sqlProfileMap.put( AppConsts.COL_USR_AGE, user.getAnalyz_age());
		}
		if(user.getAnalyz_groups() != null && !user.getAnalyz_groups().isEmpty()){
			StringBuilder sb = new StringBuilder().append(user.getAnalyz_groups().get(0));
			for(int i = 1; i < user.getAnalyz_groups().size(); i++){
				sb.append(StringUtil.DELIMIT_1ST).append(user.getAnalyz_groups().get(i));
			}
			valMap.put( AppConsts.COL_USR_GROUP, sb.toString());
			sqlProfileMap.put( AppConsts.COL_USR_GROUP, sb.toString());
		}
		if(user.getAnalyz_evidences() != null && !user.getAnalyz_evidences().isEmpty()){
			StringBuilder sb = new StringBuilder();
			// sql evidence format: IT#1335831259#wb#111641109333681$content|...
			// every group only one evidence, but now get the first one, not the latest
			HashSet<String> containGroupSet = new HashSet<String>();
			for(Entry<String, List<UserFootPrint>> entry : user.getAnalyz_evidences().entrySet()){
				for(UserFootPrint fp : entry.getValue()){
					//组成hbase evidence
					StringBuilder evdColName = new StringBuilder();
					evdColName.append(entry.getKey()).append(StringUtil.DELIMIT_1ST);
					evdColName.append(fp.getPublishTime()).append(StringUtil.DELIMIT_1ST);
					evdColName.append(fp.getDataType()).append(StringUtil.DELIMIT_1ST);
					evdColName.append(fp.getId());
					valMap.put("evidence:"+evdColName.toString(), fp.getContent());
					
					//组成sql evidence
					if (!containGroupSet.contains(entry.getKey())) {
						String evdprefix = evdColName.toString();
						sb.append(evdprefix.replaceAll(StringUtil.STR_DELIMIT_1ST, ""
								+ StringUtil.DELIMIT_3RD)).append(
								StringUtil.DELIMIT_2ND);
						String evdContent = fp.getContent();
						evdContent = evdContent.replaceAll("[$#]", " ");
						evdContent = evdContent.replace("|", "/");
						evdContent = evdContent.replaceAll("(\r|\n)", "");
						sb.append(evdContent).append(StringUtil.DELIMIT_1ST);
						
						containGroupSet.add(entry.getKey());
					}
				}
			}
//			sqlProfileMap.put(AppConsts.COL_USR_EVIDENCE, AppUtil.stripSpecialSqlChar(sb.toString()));
		}
		
		List<Put> tblPutOprs = new LinkedList<Put>();
		Put put = AppUtil.putRow2batchUpdt(user.getWeiboType() + StringUtil.DELIMIT_1ST 
				+ user.getUid(), valMap);
		if(put != null){
			tblPutOprs.add(put);
		}	 
		//写fp
		if(user.getAnalyz_filtertype() != null && !user.getAnalyz_filtertype().isEmpty()){
			for(Entry<String, WeiboFilterOp.WB_TYPE> entry : user.getAnalyz_filtertype().entrySet()){
				valMap.clear();
				valMap.put( AppConsts.COL_USR_FILTERTYPE , Integer.toString(entry.getValue().getCode()));
				tblPutOprs.add(AppUtil.putRow2batchUpdt(entry.getKey(), valMap));
			}
		}
		return new Pair<List<Put>, Map<String,String>>(tblPutOprs, sqlProfileMap);
	}
}
