package com.yeezhao.analyz.age.comparator;

import java.util.Comparator;

import com.yeezhao.analyz.age.analyzer.utility.CompareValue;
import com.yeezhao.analyz.age.user.User;

public class UserIdCmp implements Comparator<User> {
	public int compare(User obj1, User obj2) {
		return CompareValue.acs(obj1.getUserId().hashCode(), obj2.getUserId().hashCode());	//升序
	}
}