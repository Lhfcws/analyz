package com.yeezhao.analyz.age;

import java.util.ArrayList;
import java.util.List;

import com.yeezhao.analyz.age.user.User;
import com.yeezhao.analyz.age.user.UserWeibo;

public class AgeAdapter {
	
	public static User getUser(List<String> textList, List<String> tagList) {
		User user = new User("");
		List<UserWeibo> userWeibos = new ArrayList<UserWeibo>();
		for (String text : textList) {
			userWeibos.add(new UserWeibo("", text));
		}
		user.setWeibos(userWeibos);
		user.setTag(tagList);
		return user;
	}
}
