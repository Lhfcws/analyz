package com.yeezhao.analyz.filter;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.ILineParser;
import com.yeezhao.commons.util.Pair;
import com.yeezhao.commons.util.StringUtil;

/**
 * Keyword-based user name filter
 * <keyword>|<match position>, where <match position> =
 * 0: any position
 * 1: matching all
 * 2: prefix match
 * 3: suffix match
 * @author Arber
 */
public class UNameFilter implements  ILineParser{
	private static UNameFilter filter;
	private static int maxMarkLen = Integer.MIN_VALUE;
	private static int minMarkLen = Integer.MAX_VALUE;
	private static int MATCH_ANY_POS = 0;
	private static int MATCH_ALL = 1;
	private static int MATCH_PREFIX = 2;
	private static int MATCH_SUFFIX = 3;
	
	//filter keywords map
	private Map<String, Integer> kwFilterMap = new HashMap<String, Integer>();
	
	private UNameFilter(){}
	
	public static UNameFilter initFilter(InputStream adOrgMarkFile) throws IOException{
		if(filter != null)
			return filter;
		
		filter = new UNameFilter(); 
		AdvFile.loadFileInDelimitLine(adOrgMarkFile, filter);
		return filter;
	}

	public boolean filter(String name) {
		if(name == null || name.isEmpty())
			return false;
		List<Pair<Integer, Integer>> posList = StringUtil.backwardMaxMatch(name, kwFilterMap, maxMarkLen, minMarkLen);
		if(!posList.isEmpty())
			for(Pair<Integer, Integer> pair : posList){
				String kw = name.substring(pair.first, pair.second);
				int matchPos = kwFilterMap.get(kw); 
				if( ( matchPos == MATCH_ANY_POS ) || 
						( matchPos == MATCH_ALL && name.equals(kw) ) ||
						( matchPos == MATCH_PREFIX && pair.first == 0 ) ||
						( matchPos == MATCH_SUFFIX && pair.second == name.length() ) )
					return true;
			}
		return false;
	}

	@Override
	public void parseLine(String line) {
		String[] segs = line.split(StringUtil.STR_DELIMIT_1ST);
		if( segs.length < 2) return;
		try{
			kwFilterMap.put(segs[0], Integer.parseInt(segs[1]) );
			if(segs[0].length() > maxMarkLen) maxMarkLen = segs[0].length();
			if(segs[0].length() < minMarkLen) minMarkLen = segs[0].length();
		}
		catch(NumberFormatException ex){
			return;
		}
	}	
}
