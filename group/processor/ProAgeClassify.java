package com.yeezhao.analyz.group.processor;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.yeezhao.analyz.age.AgeAdapter;
import com.yeezhao.analyz.age.classifier.AgeClassifier;
import com.yeezhao.analyz.filter.WeiboFilterOp;
import com.yeezhao.analyz.group.UserFootPrint;
import com.yeezhao.analyz.group.WeiboUser;
import com.yeezhao.analyz.util.AppConsts;

public class ProAgeClassify extends BaseUserProcessor {

	private AgeClassifier ageClassifier;
	
	public ProAgeClassify(Configuration conf) throws Exception{
		super(conf);
		init(conf);
	}
	
	public ProAgeClassify(BaseUserProcessor innerProcessor) throws Exception{
		super(innerProcessor);
		init(getProConf());
	}
	
	private void init(Configuration conf) throws Exception{
		String path = conf.getResource(conf.get("age.dir")).toString();
		path = path.substring(path.indexOf(":") + 1) + File.separatorChar;

		String dataFileName = path + conf.get("age.NB.data.file");
		String modelFileName = path + conf.get("age.NB.model.file");
		String attriWordFileName = path + conf.get("age.attri.word.file");
		ageClassifier = new AgeClassifier(dataFileName, modelFileName,
				attriWordFileName);
	}
	
	@Override
	public WeiboUser processMethod(WeiboUser user) {
		Iterator<String> itor = user.getFootPrints().keySet().iterator();
		List<String> textList = new ArrayList<String>();
		List<String> tagList = new ArrayList<String>();
		while (itor.hasNext()) {
			String key = itor.next();
			UserFootPrint fp = new UserFootPrint(user.getFootPrints().get(key));
			if(user.getAnalyz_filtertype() != null
					&& user.getAnalyz_filtertype().get(key) != WeiboFilterOp.WB_TYPE.NORMAL){
				continue;
			}
			if (fp.getDataType().equals(AppConsts.FP_MSG)) {
				textList.add(fp.getContent());
			} else if (fp.getDataType().equals(AppConsts.FP_TAG)) {
				tagList.addAll(Arrays.asList(fp.getContent().split("[|]")));
			}
		}
		String resAge = ageClassifier.classify(AgeAdapter.getUser(textList, tagList));
		user.setAnalyz_age(resAge);
		
		return user;
	}

	@Override
	public boolean validateParams(WeiboUser user) {
		if (user.getFootPrints().isEmpty())
			return false;
		return true;
	}

}
