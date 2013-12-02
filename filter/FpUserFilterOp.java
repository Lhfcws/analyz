package com.yeezhao.analyz.filter;

import com.yeezhao.amkt.core.CoreConsts.USER_TYPE;
import com.yeezhao.analyz.util.AppConsts;
import com.yeezhao.commons.classifier.base.Classifier;
import com.yeezhao.commons.classifier.base.DocProcessor;
import com.yeezhao.commons.classifier.base.Document;

/**
 * 基于Footprint表的UserFilter
 * @author akai
 */
public class FpUserFilterOp implements Classifier, DocProcessor {
	private int limit;
	
	@Override
	public Document process(Document doc) throws Exception {
		String result = classify( doc );
		if( result != null )
			doc.putField(AppConsts.COL_USR_TYPE, result );
		return doc;
	}

	@Override
	public String classify(Document doc) {
		USER_TYPE userType = USER_TYPE.NORMAL;
		long max=doc.containsField("lastWeiboTime")? Integer.parseInt( doc.get( "lastWeiboTime" ) ) : 0;
		if (System.currentTimeMillis()-(max*1000)>limit*24*60*60*1000l) {
			userType= USER_TYPE.ROBOT;
		}else{
			userType= USER_TYPE.NORMAL;
		}
		return Integer.toString(userType.getCode());
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

//	private int getIntVal( Document doc, String fieldName ){
//		try{
//			return doc.containsField(fieldName)? Integer.parseInt( doc.get( fieldName ) ) : -1;
//		}
//		catch( NumberFormatException ex ){
//			return  -1;
//		}
//	}
}
