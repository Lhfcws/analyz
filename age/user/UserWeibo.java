package com.yeezhao.analyz.age.user;

/**
 * 用户发的微博
 * @author summba
 *
 */
public class UserWeibo {
	private String id;
	private String text;
	public UserWeibo(String id, String text) {
		super();
		this.id = id;
		this.text = text;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
}
