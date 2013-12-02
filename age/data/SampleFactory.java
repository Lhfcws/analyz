package com.yeezhao.analyz.age.data;

import java.util.List;

import com.yeezhao.analyz.age.analyzer.weibo.Weibo;

public class SampleFactory {

	public Sample createTrainSample(List<Weibo> weiboList) {
		return new SampleCreator().createSample(weiboList);
	}
	public Sample createPredictSample(Sample trainSample, List<Weibo> weiboList) {
		return new SampleCreator().createSample(trainSample, weiboList);
	}
}
