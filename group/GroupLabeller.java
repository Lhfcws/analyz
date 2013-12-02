package com.yeezhao.analyz.group;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.sjb.ontology.FileHandler;
import com.yeezhao.commons.hbase.HBaseUtil;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.analyz.util.AnalyzConfiguration;
import com.yeezhao.analyz.util.AppConsts;

public class GroupLabeller {

	private GroupLabelerCache labelerCache;
	private static final String FP_UID_PREFIX = "@uid";
	
	public GroupLabeller(Configuration conf) throws IOException{
		this.labelerCache = new GroupLabelerCache(conf.getConfResourceAsInputStream(conf.get(AppConsts.GROUP_MODEL_FILE)));
	}
	
	/**
	 * 一个group必须要有一条weibo evidence。如果group是从tag分析得到，则从
	 * 用户的所有微博中选择一条最相近的，或者第一条; 
	 * @param resultLabels
	 * @param orgUserData
	 */
	private void refineEvidences(Map<String, List<String>> resultLabels, 
			Map<String, UserFootPrint> orgUserData){
		if(resultLabels == null || resultLabels.isEmpty())
			return;
		
		Iterator<String> grpItor = resultLabels.keySet().iterator();
		StringBuffer grps = new StringBuffer();
		while(grpItor.hasNext()){
			String grpKey = grpItor.next();
			List<String> evdList = resultLabels.get(grpKey);
			String wbEvd = null;
			for(String evd : evdList){
				if(orgUserData.get(evd).getDataType().equals(AppConsts.FP_MSG)){
					wbEvd = evd;
				} 
			}
			if(wbEvd == null && (wbEvd = indirectMsgEvd(grpKey, orgUserData)) == null){
				grpItor.remove();	//如果没有找到证据则直接删除这个group
				continue;
			}
			grps.append(grpKey).append(StringUtil.DELIMIT_1ST);
			evdList.remove(wbEvd);
			evdList.add(0, wbEvd); //将weibo evidence排在最前面
		}
	}
	
	/**
	 * 当没有直接的weibo evidence的时候，从所有微博中挑选合适的微博作为间接evidence。
	 * @param groupName
	 * @param orgDataMap
	 * @return 如果仍然没有找到，返回null
	 */
	private String indirectMsgEvd(String groupName, Map<String, UserFootPrint> orgDataMap){
		Map<String, List<String>> kwMsgMap = new HashMap<String, List<String>>();
		KeywordMatchLabeller.decapsulateKwMap(labelerCache.getUserGroup(groupName), kwMsgMap);
		KeywordMatchLabeller labeler = new KeywordMatchLabeller(UserGroup.SrcType.MSG,
				kwMsgMap);
		Map<String, List<String>> susMap = labeler.analyzUserData(orgDataMap);
		if(susMap.get(groupName) != null)
			return susMap.get(groupName).get(0);
		String[] msgs = orgDataMap.keySet().toArray(new String[orgDataMap.size()]);
		if(msgs.length == 0){
			return null;
		} else if(orgDataMap.get(msgs[0]).getDataType().equals(AppConsts.FP_MSG)){
			return msgs[0];
		} else if(msgs.length > 1){
			return msgs[1];
		}
		return null;
	}

	/**
	 * 
	 * @param printMap <footPrintNum, FootPrint>
	 * @return <userGroup, list of footPrintNums>
	 */
	public Map<String, List<String>> labelFootPrints(Map<String, UserFootPrint> printMap){
		Map<String, List<String>> label2Fp = new HashMap<String, List<String>>();
		for(GroupLabel groupLabeler : labelerCache.listUserGroups()){
			Map<String, List<String>> valueMap = groupLabeler.analyzUserData(printMap);
			for(Entry<String, List<String>> entry : valueMap.entrySet()){
				if(label2Fp.containsKey(entry.getKey())){
					label2Fp.get(entry.getKey()).addAll(entry.getValue()); //TODO: 没有考虑重复的情况
				} else{
					label2Fp.put(entry.getKey(), entry.getValue());
				}
			}
		}
		
		refineEvidences(label2Fp, printMap);
		return label2Fp;
	}
	
	private void groupLabel(Map<String, UserFootPrint> fpMap, String uid){
		Map<String, List<String>> grp2Fps = labelFootPrints(fpMap);
		if(!grp2Fps.isEmpty()){
			boolean hasWeiboEvd = false;
			StringBuffer sb = new StringBuffer();
			sb.append("user").append(uid).append("\n");
			for(Entry<String, List<String>> entry : grp2Fps.entrySet()){
				boolean validGrp = false;
				StringBuffer evdBuf = new StringBuffer();
				evdBuf.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
				for(String evd : entry.getValue()){
					if(fpMap.get(evd).getDataType().equals(AppConsts.FP_MSG)){
						validGrp = true;
						hasWeiboEvd = true;
					}
				}
				if(validGrp)
					sb.append(evdBuf.toString());
			}
			if(hasWeiboEvd)
				System.out.println(sb.toString());
		}
	}
	
	/**
	 * 从文本中加载用户数据,分析用户标签
	 * @param inputFile
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void testFromFile(String inputFile) throws FileNotFoundException, IOException{
		List<String> lines = FileHandler.readLinesIntoList(new FileInputStream(inputFile));
		String uid = null;
		int wid = 0;
		Map<String, UserFootPrint> fpMap = new HashMap<String, UserFootPrint>();
		for(int i = 0, l = lines.size(); i < l; i++){
			String line = lines.get(i);
			if(line.startsWith(FP_UID_PREFIX)){
				if(i != 0){
					groupLabel(fpMap, uid);
					fpMap.clear();
				}
				uid = line.substring(line.indexOf(FP_UID_PREFIX) + FP_UID_PREFIX.length());
				wid = 0;
			} else{
				UserFootPrint fp = new UserFootPrint();
				fp.setDataType(line.substring(0, line.indexOf(",")));
				fp.setContent(line.substring(line.indexOf(",") + 1));
				fpMap.put(Integer.toString(++wid), fp);
			}
		}
	}
	
	/**
	 * 
	 * @param uid, 必须包含前缀名,例如 sn|32847283564
	 * @throws IOException 
	 */
	public void testFromHbase(String uid) throws IOException{
		Configuration conf = AnalyzConfiguration.getInstance();
		HTable htable = new HTable(conf, conf.get(AppConsts.HTBL_USER_PROFILE) );
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("fp"), Bytes.toBytes("content"));
		scan.setStartRow(Bytes.toBytes(uid));
		scan.setStopRow(Bytes.toBytes(HBaseUtil.getPrefixUpperBound(uid)));
		ResultScanner scanner = null;
		try{
			scanner = htable.getScanner(scan);
			Result rs = null;
			Map<String, UserFootPrint> fpMap = new HashMap<String, UserFootPrint>();
			while((rs = scanner.next()) != null){
				String key = Bytes.toString(rs.getRow());
				String content = Bytes.toString(rs.getValue(Bytes.toBytes("fp"), Bytes.toBytes("content")));
				UserFootPrint fp = new UserFootPrint();
				fp.setDataType(key.split(StringUtil.STR_DELIMIT_1ST)[2]);
				fp.setContent(content);
				fpMap.put(key, fp);
			}
			
			groupLabel(fpMap, uid);
		} catch(Exception e){
			e.printStackTrace();
		} finally{
			scanner.close();
		}
		
	}

	public static void main(String[] args) throws IOException {
		if(args.length != 2){
			System.out.println("GroupLabeller Usage: " +
					"-file <fileName>\n" +
					"-hbase <userId>");
		}
		Configuration conf = AnalyzConfiguration.getInstance();
		GroupLabeller labeller = new GroupLabeller(conf);
		
		if(args[0].equals("-file"))
			labeller.testFromFile(args[1]);
		else if(args[0].equals("-hbase"))
			labeller.testFromHbase(args[1]);
	}
}