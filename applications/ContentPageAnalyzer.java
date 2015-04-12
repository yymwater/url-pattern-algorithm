package com.flytek.urlpattern.tree.applications;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.flytek.urlpattern.tree.TreeNode;
import com.iflytek.parse.html.analyzers.IAnalyzer;
import com.iflytek.parse.html.commons.HtmlParseInfo;
import com.iflytek.parse.html.commons.PageType;

public class ContentPageAnalyzer implements IAnalyzer{
	
	private HashMap<String, TreeNode> host2tn; 
	
	double min_support;
	
	
	public void setMinSupport(double min_support){
		this.min_support = min_support;
	}
	
	public void analyze(HtmlParseInfo parseInfo, Map<String,Object>references){
		TreeNode tn = null;
		if(host2tn.containsKey(parseInfo.getUrl().toString())){
			tn = host2tn.get(parseInfo.getUrl().toString());
		}else{
//			loadFile()
		}
		String url = parseInfo.getUrl().toString();
		List<TreeNode> leafs = new ArrayList<TreeNode>();
		getLeaf(tn, leafs);
		int totalsupport = 0;
		int virtualsupport = 0;
		for(TreeNode leaf: leafs){
			int thisleafsupport = leaf.getUrlsList().size();
			totalsupport = totalsupport + thisleafsupport;
//			if(leaf.matchByRegex(url)){
//				System.out.println(leaf.getPatternWithoutKeys(leaf.getPatternWithKeys()));
//				virtualsupport = virtualsupport + thisleafsupport;
//			}
		}
		
		double support = 0;
		
		if(totalsupport ==0){
			System.out.println("error");
			return;
		}else{
			support = virtualsupport*1.0/totalsupport;
		}
		if(support >= min_support){
			 parseInfo.setPageType(PageType.THEME_PAGE);
		}
			
	}
	
	public void getLeaf(TreeNode tn, List<TreeNode> leafs){
		
		if(tn.getChildNodes() == null){
			leafs.add(tn);
		}else{
			List<TreeNode> childNodes = tn.getChildNodes();
			for(TreeNode tn1: childNodes){
				getLeaf(tn1, leafs);
			}
		}

	}
}
