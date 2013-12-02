package com.yeezhao.analyz.age.comparator;

import java.util.Comparator;

import com.yeezhao.analyz.age.analyzer.utility.CompareValue;
import com.yeezhao.analyz.age.user.User;

public class UserAgeDescCmp implements Comparator<User> {
	public int compare(User obj1, User obj2) {
		return CompareValue.desc(obj1.getAge(), obj2.getAge());	//降序
	}
}
