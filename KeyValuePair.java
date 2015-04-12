package com.flytek.urlpattern.tree;

import java.io.Serializable;

/**
 * author:Yang Yiming
 * Key-Value对，用于url重写过程
 * date:2014.05.23
 * **/
public class KeyValuePair implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -8876200866806874493L;
	private String keyStr;
	private String valueStr;
	public KeyValuePair(){
	}
	public KeyValuePair(String keyStr, String valueStr) {
		this.keyStr = keyStr;
		this.valueStr = valueStr;
	}
	public String getKeyStr() {
		return keyStr;
	}
	public void setKeyStr(String keyStr) {
		this.keyStr = keyStr;
	}
	public String getValueStr() {
		return valueStr;
	}
	public void setValueStr(String valueStr) {
		this.valueStr = valueStr;
	}
	public String toString() {
		return keyStr+":"+valueStr;
	}
}
