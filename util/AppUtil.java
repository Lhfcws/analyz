package com.yeezhao.analyz.util;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;

import com.yeezhao.amkt.core.CoreConsts.USER_TYPE;
import com.yeezhao.commons.classifier.base.Document;
import com.yeezhao.commons.util.StringUtil;

public class AppUtil {
	public static double exp = 10E-10;
	private static int MIN_TWEET_NUM = 10;

	// Get the user source, i.e. sn/tx, from a user info/footprint rowkey
	public static String getUsrSrc(String userRowkey) {
		return userRowkey.split(StringUtil.STR_DELIMIT_1ST)[0];
	}

	// FanID --> User info rowkey, i.e. "2891168265" --> "sn|2891168265"
	public static byte[] fanID2UInfoRowkey(byte[] uFanRowkey, byte[] fanID) {
		return Bytes.toBytes(String.format("%s|%s",
				getUsrSrc(Bytes.toString(uFanRowkey)), Bytes.toString(fanID)));
	}
	
	// FanID --> User info rowkey, i.e. "2891168265" --> "sn|2891168265|wb"
	public static String fanID2FootprintRowkey(byte[] uFanRowkey, byte[] fanID,
			String dataType) {
		return String.format("%s|%s|%s", getUsrSrc(Bytes.toString(uFanRowkey)),
				Bytes.toString(fanID), dataType);
	}

	// Timestamp in seconds to the hour of day
	public static int getHourByTimestamp(long ts) {
		Calendar pubDate = Calendar.getInstance();
		pubDate.setTimeInMillis(ts * 1000);
		return pubDate.get(Calendar.HOUR_OF_DAY);
	}

	public static void normalizeBySum( double[] dist ){
		double sum = 0.0f;
		for( int i = 0; i < dist.length; i++ ) sum += dist[i];
		if( sum > exp )
			normalizeBySum( dist, sum );
	}
	
	private static void normalizeBySum( double[] dist, double base ){
		for( int i = 0; i < dist.length; i++ ){
			dist[i] = dist[i] / base;
		}
	}
	
	public static String genDelimitedString( double[] dList , char delimit ){
		if( dList == null || dList.length < 1 ) return "";
		StringBuilder sb = new StringBuilder();
		for( double d: dList){
			sb.append( Double.toString(d) );
			sb.append(delimit);
		}
		return sb.substring(0, sb.length() - 1 );		
		
	}
	
	
	/**
	 * 根据发微薄的时间来确定用户活跃度，算法: 1. x = 微薄覆盖的总天数; y = 实际有发微薄的天数 2. 活跃度 = (x/y) * 10
	 * 
	 * @param tsList
	 *            a list of tweet post time
	 * @param minTwNum
	 *            minimum tweet number to determine user activity level
	 * @return [0-10] indicating the user activity level, -1 means unknown ( not
	 *         enough data to determine )
	 */
	public static int getUsrActiveLvl(List<Long> tsList, int minTwNum) {
		if (tsList.size() < minTwNum)
			return -1;
		SortedSet<Long> twDaySet = new TreeSet<Long>();
		for (Long ts : tsList)
			twDaySet.add(AppUtil.getDayByTimestamp(ts));

		Long startDay = twDaySet.first();
		Long lastDay = twDaySet.last();
		return Math.round(twDaySet.size() * 10
				/ (float) (lastDay - startDay + 1));
	}
	
	public static int getUsrActiveLvl( List<Long> tsList ){
		return getUsrActiveLvl( tsList, MIN_TWEET_NUM );
	}

	// Convert timestamp in seconds into days
	public static long getDayByTimestamp(long ts) {
		return ts / (3600 * 24);
	}

	/**
	 * Function for extracting the weibo terminal from a string like the
	 * following: Source [url=http://www.huaweidevice.com/cn/,
	 * relationShip=nofollow, name=华为云手机]
	 * 
	 * @param terminalString
	 * @return the terminal
	 */
	public static String extractWbTerminal(String terminalString) {
		int pos = terminalString.indexOf("name=");
		if (pos == -1)
			return AppConsts.VAL_UNK;
		pos += "name=".length();
		return (terminalString.length() - pos < 3) ? AppConsts.VAL_UNK
				: terminalString.substring(pos, terminalString.length() - 1);
	}

	/**
	 * Given a number, return its order of magnitude, i.e. 3000 --> 1000 35000
	 * --> 10000
	 * 
	 * @param num
	 * @return return the order of magnitude
	 */
	public static int getOrderMagnitude(int num) {
		return num <= 0 ? -1 : (int) Math.pow(10, Math.floor(Math.log10(num)));
	}

	/**
	 * 将文本内容按行平均写入N个hdfs文件中, 并且返回这些文件的上一级目录。
	 * 
	 * @param conf
	 * @param outputDir
	 * @param lines
	 * @param files
	 * @return
	 * @throws IOException
	 */
	public static Path generateHdfsSplits(Configuration conf, String outputDir,
			List<String> lines, int files) throws IOException {
		String dir = outputDir;
		int count = 0;
		String filePrefix = "keys-";
		int fileNum = 0;
		FileSystem fs = FileSystem.get(java.net.URI.create(dir), conf);
		OutputStream out = fs.create(new Path(dir + "/" + filePrefix + fileNum
				+ ".txt"));
		int linesPerFile = lines.size() / files;
		for (int i = 0, l = lines.size(); i < l; i++) {
			out.write((lines.get(i) + "\n").getBytes());
			count++;
			if (count == linesPerFile) {
				count = 0;
				fileNum++;
				out.close();
				out = fs.create(new Path(dir + "/" + filePrefix + fileNum
						+ ".txt"));
			}
		}
		if (count != 0)
			out.close();
		Path rootUrlDir = new Path(dir);
		return rootUrlDir;
	}

	/**
	 * 主要是为了footprint rowkey split后的长度一致的问题 src|uid|wb|data_id =4 (String.split)
	 * src|uid|tag| = 3 ( String.split )
	 * 
	 * @param rowkey
	 * @param delimit
	 * @return
	 */
	public static String[] splitFpRowKey(String rowkey, String regx) {
		String suffix = "E";
		rowkey += suffix;
		String[] segs = rowkey.split(regx);
		String suffixSeg = segs[segs.length - 1];
		segs[segs.length - 1] = suffixSeg.substring(0, suffixSeg.length() - 1);
		return segs;
	}

	public static String getColumnValue(Result row, String column,
			String... defaultValue) {
		int pos = column.indexOf(":");
		if (pos == -1)
			return defaultValue.length == 0 ? null : defaultValue[0];
		byte[] res = row.getValue(Bytes.toBytes(column.substring(0, pos)),
				Bytes.toBytes(column.substring(pos + 1)));
		if (res == null)
			return defaultValue.length == 0 ? null : defaultValue[0];
		return Bytes.toString(res).trim();
	}
	
	public static String getColumnValue(Result row, byte[] family, byte[] qualifier,
			String... defaultValue) {
		byte[] res = row.getValue(family, qualifier);
		if (res == null)
			return defaultValue.length == 0 ? null : defaultValue[0];
		return Bytes.toString(res).trim();
	}

	/**
	 * 将生成的附加信息map添加到BatchUpdate
	 */
	public static Put putRow2batchUpdt(String rowkey,
			Map<String, String> col2val) throws IOException {
		if (col2val.isEmpty())
			return null;
		Put update = new Put(Bytes.toBytes(rowkey));
		for (Map.Entry<String, String> e : col2val.entrySet()) {
			String[] subs = e.getKey().split(":");
			update.add(Bytes.toBytes(subs[0]), Bytes.toBytes(subs[1]),
					Bytes.toBytes(e.getValue()));
		}
		return update;
	}

	/**
	 * 
	 * @param time
	 *            , 精确到秒
	 * @return
	 */
	public static String time2String(long time) {
		time *= 1000;
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return format.format(new Date(time));
	}

	/**
	 * 
	 * @param time
	 *            , 格式例如 2012-04-12 12:23:45
	 * @return 返回时间的long表示，精确到秒
	 * @throws ParseException
	 */
	public static long time2long(String time) throws ParseException {
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return format.parse(time).getTime() / 1000;
	}

	public static void main(String[] args) throws UnknownHostException {
		String ipString = InetAddress.getLocalHost().getHostAddress();
		System.out.println(ipString);
	}

	/**
	 * 返回当前jvm所在server的ip地址。
	 * 
	 * @return
	 */
	public static String getMailSenderIpAddress() {
		try {
			String ip = InetAddress.getLocalHost().getHostAddress();
			return ip;
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "";
		}
	}

	/**
	 * 产生数据库插入语句
	 * 
	 * @param DB_TBL
	 * @param attrVals
	 * @return
	 */
	public static String generateInsertExpr(String DB_TBL,
			Map<String, String> attrVals) {
		if (attrVals.isEmpty())
			return null;
		StringBuilder attrs = new StringBuilder(), vals = new StringBuilder();
		Iterator<String> itor = attrVals.keySet().iterator();
		String key = itor.next();
		String[] subs = key.split(":");  //将带:进行分隔
		String column = subs.length > 1 ? subs[1] : subs[0];
		attrs.append(column);
		while (itor.hasNext()) {
			key = itor.next();
			subs = key.split(":");  //将带:进行分隔
			column = subs.length > 1 ? subs[1] : subs[0];
			attrs.append("," + column);
		}

		itor = attrVals.values().iterator();
		String firstVal = itor.next();
		firstVal = firstVal == null ? null : "'" + charConvert(firstVal)
				+ "'";
		vals.append(firstVal);
		while (itor.hasNext()) {
			String val = itor.next();
			val = "," + (val == null ? null : "'" + charConvert(val) + "'");
			vals.append(val);
		}
		return String.format("INSERT INTO %s (%s) VALUES(%s)", DB_TBL,
				attrs.toString(), vals.toString());
	}

	public static String charConvert(String orgValue) {
		orgValue = orgValue.replaceAll("\\\\", "\\\\\\\\");
		//orgValue = orgValue.replaceAll("\"", "\\\\\"");
		orgValue = orgValue.replaceAll("'", "\\\\'");
		return orgValue;
	}

	/**
	 * 产生数据库更新语句中的更新值部分，需要添加前后的判定条件
	 * 
	 * @param attrVals
	 * @return
	 */
	public static String generateUpdtExpr(Map<String, String> attrVals) {
		if (attrVals.isEmpty())
			return null;
		StringBuilder sb = new StringBuilder();
		Iterator<Map.Entry<String, String>> itor = attrVals.entrySet()
				.iterator();
		Map.Entry<String, String> entry = itor.next();
		String val = entry.getValue() == null ? null : "'"
				+ charConvert(entry.getValue()) + "'";
		String[] subs = entry.getKey().split(":");  //将带:进行分隔
		String column = subs.length > 1 ? subs[1] : subs[0];
		sb.append(column).append("=").append(val);
		while (itor.hasNext()) {
			entry = itor.next();
			subs = entry.getKey().split(":");  //将带:进行分隔
			column = subs.length > 1 ? subs[1] : subs[0];
			val = entry.getValue() == null ? null : "'"
					+ charConvert(entry.getValue()) + "'";
			sb.append(",").append(column).append("=").append(val);
		}
		return sb.toString();
	}

	public static String stripSpecialSqlChar(String sqlvalue) {
		if (sqlvalue == null || sqlvalue.isEmpty())
			return null;
		sqlvalue = sqlvalue.replaceAll("['\uD800\uDC00-\uDBFF\uDFFF]", ""); // 去掉特殊字符',因为在preparedStatement中默认采用''连接字段值
																			// 以及unicode的U+10000-U+10FFFF之间的supplementary
																			// character
		return sqlvalue;
	}

	public static String extractEvidenceString(Result rs) {
		Map<byte[], byte[]> colMap = rs.getFamilyMap(Bytes.toBytes("evidence"));
		if (colMap != null && !colMap.isEmpty()) {
			StringBuffer sb = new StringBuffer();
			// evidence format: IT#1335831259#wb#111641109333681$content|...
			for (Entry<byte[], byte[]> evdEntry : colMap.entrySet()) {
				String evdprefix = Bytes.toString(evdEntry.getKey());
				sb.append(
						evdprefix.replaceAll(StringUtil.STR_DELIMIT_1ST, ""
								+ StringUtil.DELIMIT_3RD)).append(
						StringUtil.DELIMIT_2ND);
				String evdContent = Bytes.toString(evdEntry.getValue());
				evdContent = evdContent.replaceAll("[$#]", " ");
				evdContent = evdContent.replace("|", "/");
				sb.append(evdContent).append(StringUtil.DELIMIT_1ST);
			}
			return AppUtil.stripSpecialSqlChar(sb.toString());
		}
		return null;
	}

	/**
	 * Convert a row object into document key-val pairs
	 * 
	 * @param row
	 * @return
	 */
	public static Document row2Doc(Result row) {
		Document doc = new Document(Bytes.toString(row.getRow()));
		for (KeyValue kv : row.list()) {
			String key = Bytes.toString(kv.getFamily()) + ":"
					+ Bytes.toString(kv.getQualifier());
			doc.putField(key, Bytes.toString(kv.getValue()));
		}
		return doc;
	}
	
	/**
	 * 如果user_type等于0/1/null，返回为true;否则返回为false
	 * @param userTypeValue
	 * @param normalCounter
	 * @param orgCounter
	 * @param robotCounter
	 * @param unknownTypeCounter
	 * @return
	 */
	public static boolean validateUser(byte[] userTypeValue, Counter normalCounter, Counter orgCounter, 
			Counter robotCounter, Counter unknownTypeCounter){
		if(userTypeValue == null){
			unknownTypeCounter.increment(1);
			return false;
		} else {
			try{
				int val = Integer.parseInt(Bytes.toString(userTypeValue));
				if(val == USER_TYPE.NORMAL.getCode()){
					normalCounter.increment(1);
				} else if(val == USER_TYPE.ORG.getCode()){
					orgCounter.increment(1);
				} else if(val == USER_TYPE.ROBOT.getCode()){
					robotCounter.increment(1);
					return false;
				} else {
					unknownTypeCounter.increment(1);
					return false;
				}
			} catch (Exception e){
				unknownTypeCounter.increment(1);
				return false;
			}
		}
		return true;
	}
}
