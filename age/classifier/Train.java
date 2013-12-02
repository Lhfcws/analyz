package com.yeezhao.analyz.age.classifier;
/**
 * @author yunbiao
 *
 */
public class Train {
	
	public static void main(String[] args) {
		AgeClassifier ac = new AgeClassifier("e:\\age\\attri_word.txt");
		ac.trainAndPredict("e:\\age\\weibo_before_2000_tx_userWeibo.txt");
	}

}
