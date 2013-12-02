package com.yeezhao.analyz.age.analyzer.utility;

public class CompareValue {

	/**
	 * 返回升序列
	 * @param num1
	 * @param num2
	 * @return
	 */
	public static int acs(double num1, double num2) {
		if (num2 > num1) {		//升序
			return -1;			
		}
		else if (num2 < num1) {
			return 1;
		}else {
			return 0;			
		}
	}
	/**
	 * 返回降序
	 * @param num1
	 * @param num2
	 * @return
	 */
	public static int desc(double num1, double num2) {
		if (num2 > num1) {		//升序
			return 1;			
		}
		else if (num2 < num1) {
			return -1;
		}else {
			return 0;			
		}
	}
}
