package com.yeezhao.analyz.age;

import java.util.ArrayList;
import java.util.List;

import com.yeezhao.analyz.age.analyzer.utility.FileHandler;
import com.yeezhao.analyz.age.classifier.AgeClassifier;

/**
 * 
 * @author user
 *
 */
public class AgeClassifyPerformance {
	
	/**
	 * 可以用这个sql语句从数据库中拿出birth_date和age两列:
	 * mysql -h192.168.1.122 -uweibo -p123456 -e "use weibodb_dev_1;select birth_date,
	 * 	age from t_user_info where age > 0 and (birth_date like '1%' or birth_date 
	 *  like '2%') limit 0,100000" > tmp
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length !=  2){
			System.out.println("Usage: PerformanceTester -i <file>");
			System.out.println("     format for userBirthdayInfoFile is as follows:" +
							   "	 birth_date	age");
			System.exit(1);
		}
		if(!args[0].equals("-i"))
			return;
		List<String> fileList = FileHandler.readFileToList(args[1]);
		List<String> orgs = new ArrayList<String>();
		List<String> results = new ArrayList<String>();
		for (String line : fileList) {
			String[] split = line.split("\\s+");
			if (split.length != 2) {
				System.err.println(line);
				continue;
			}
			String year = split[0].split("[-]")[0];
			if (Integer.valueOf(year) > 0) {
				orgs.add(AgeClassifier.getAgeLabel(Integer.valueOf(year)));
				results.add(split[1]);
			}
		}
		String[] org = new String[orgs.size()];
		String[] result = new String[results.size()];
		for (int i = 0; i < result.length; i++) {
			org[i] = orgs.get(i);
			result[i] = results.get(i);
		}
		AgeClassifier.printStaticstic(org, result);
	}
	
}
