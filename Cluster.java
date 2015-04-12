package com.flytek.urlpattern.tree;

import java.util.ArrayList;
import java.util.List;

import com.sun.org.apache.bcel.internal.generic.NEW;
/**
 * author:Yang Yiming
 * 里面包含指向同一网页的不同url列表，用于url重写
 * **/
public class Cluster {
	List<ProcessedUrl>urlList;
	public Cluster(){
		urlList=new ArrayList<ProcessedUrl>();
	}
	public List<ProcessedUrl> getUrlList() {
		return urlList;
	}

	public void setUrlList(List<ProcessedUrl> urlList) {
		this.urlList = urlList;
	}
	public boolean hasUrl(ProcessedUrl url){
		for (ProcessedUrl processedUrl1 : urlList) {
			if (url.equals(processedUrl1)) {
				return true;
			}
		}
		return false;
	}
	public void add(String url){
		urlList.add(new ProcessedUrl(url));
	}
}	
