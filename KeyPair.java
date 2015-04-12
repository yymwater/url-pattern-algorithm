package com.flytek.urlpattern.tree;

import java.io.Serializable;
import java.util.List;
/**
 * author:Yang Yiming
 * Key-Key对，用于url重写过程
 * date:2014.05.23
 * **/
public class KeyPair implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2557044985438461656L;
	private String key1;
	private String key2;
	public KeyPair(String key1,String key2){
		this.key1=key1;
		this.key2=key2;
	}
	final public String getKey1() {
		return key1;
	}
	final public void setKey1(String key1) {
		this.key1 = key1;
	}
	final public String getKey2() {
		return key2;
	}
	final public void setKey2(String key2) {
		this.key2 = key2;
	}
	final public String toString() {
		return key1+"和"+key2;
	}
	
}
