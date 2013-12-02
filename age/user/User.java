package com.yeezhao.analyz.age.user;

import java.util.ArrayList;
import java.util.List;

/**
 * 微博用户
 * @author summba
 *
 */
public class User {
	private String userId;				// 
	private String userName;			// 
	private int age;					// 
	private List<String> tag;			// 
	private List<UserWeibo> weibos;		// 
	
	
	public User(String userId) {
		super();
		this.userId = userId;
	}
	public User(String userId, String userName, int age, List<String> tag) {
		super();
		this.userId = userId;
		this.userName = userName;
		this.age = age;
		this.tag = tag;
		this.weibos = new ArrayList<UserWeibo>();
	}	
	public void addWeibo(UserWeibo weibo) {
		this.weibos.add(weibo);
	}
	public void addWeibos(List<UserWeibo> weibos) {
		this.weibos.addAll(weibos);
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((userId == null) ? 0 : userId.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		User other = (User) obj;
		if (userId == null) {
			if (other.userId != null)
				return false;
		} else if (!userId.equals(other.userId))
			return false;
		return true;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	public List<String> getTag() {
		return tag;
	}
	public void setTag(List<String> tag) {
		this.tag = tag;
	}
	public List<UserWeibo> getWeibos() {
		return weibos;
	}
	public void setWeibos(List<UserWeibo> weibos) {
		this.weibos = weibos;
	}
}
