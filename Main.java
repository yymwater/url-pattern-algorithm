package com.flytek.urlpattern.tree;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.iflytek.fs.avro.generate.DownloadPage;
import com.iflytek.fs.avro.generate.DuplicatePages;
import com.iflytek.fs.avro.generate.RawPage;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Collections;

public class Main {
	public static HashMap<String,Pattern>map=new HashMap<String,Pattern>();
	public static boolean checkPattern(String patternStr,String hostStr){
		// 有的为空   有的只有host/* 
		if(patternStr.length()<=5){
			return false;
		}if(patternStr.length()<=hostStr.length()+6){
			return false;
		}if(patternStr.split("\\|").length==1){
			String tempArr[]=patternStr.split("#");
			boolean flag=false;
			for(int i=1;i<tempArr.length;i++){
				if(tempArr[i].length()>3){
					flag=true;
					break;
				}
			}
			if(!flag){
				return false;
			}
		}
//		if(!patternStr.contains("*"))
//			return false;
		return true;
	}
	public static HashMap<String,Integer>map2=new HashMap<String,Integer>();
	public static void getClustersFromHDFS(String fileName){
		Configuration config = new Configuration();
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(config);
			Path destFile = new Path(fileName);
			InputStream is =hdfs.open(destFile);
			DataFileStream<DownloadPage> reader = new DataFileStream<DownloadPage>(is,
					new SpecificDatumReader<DownloadPage>());
			long sum=0;
			while (reader.hasNext()) {
				DownloadPage pages = reader.next();
				System.out.println(pages.getUrl());
			}
			IOUtils.cleanup(null, is);
			IOUtils.cleanup(null, reader);
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	public static String getLeafTreeNodesPatterns1(TreeNode rootNode){
		String patternsStr="";
		Queue<TreeNode>queue=new LinkedList<TreeNode>();
		queue.add(rootNode);
		TreeNode currentNode;
		boolean isFirst=true;
		while(!queue.isEmpty()){
			currentNode=queue.poll();
			if(currentNode.getChildNodes()!=null&&currentNode.getChildNodes().size()>0){
				boolean allChildIsLeafFlag=true;
				for(TreeNode treeNode:currentNode.getChildNodes()){
					if(treeNode.getChildNodes()!=null&&treeNode.getChildNodes().size()>0){
						allChildIsLeafFlag=false;
						break;
					}
				}
				if(currentNode.isChildHasStarFlag()&&allChildIsLeafFlag){
					queue.add(currentNode.getChildNodes().get(0));
					continue;
				}
				for(TreeNode node:currentNode.getChildNodes()){
					queue.add(node);
				}
			}else{
//				if(currentNode.getPartPatternList()==null||currentNode.getPartPatternList().size()==0){
//					continue;
//				}
				String patternStr=currentNode.getPatternOutputStr();
				if(isFirst){
					patternsStr+=patternStr;
					isFirst=false;
				}else{
					patternsStr+="|"+patternStr;
				}
			}
		}
		return patternsStr;
	}
	public static void main(String arogs[]) throws IOException {
		BufferedReader reader1=new BufferedReader(new FileReader("D://test"));
		List<ProcessedUrl>list=new ArrayList<ProcessedUrl>();
		String lineStr1;
		while((lineStr1=reader1.readLine())!=null){
			list.add(new ProcessedUrl(lineStr1));
		}
		Fun fun=new Fun();
		TreeNode node=fun.buildPatternTree(list, new ArrayList<String>(), null, null);
		System.out.println("build tree end!");
		System.out.println("---------------------------------------------");
		String resultStr=getLeafTreeNodesPatterns1(node);
		System.out.println(resultStr);
		Pattern pattern1=Pattern.compile(resultStr);
		double sum11=0;
		for(ProcessedUrl url:list){
			if(pattern1.matcher(CalculateUtil.getSortedUrl(url.getUrlStr())).find())
				sum11++;
			else{
//				System.out.println(url.getUrlStr());
//				System.out.println(CalculateUtil.getSortedUrl(url.getUrlStr()));
//				System.out.println();
			}
		}
		System.out.println(sum11/list.size());
		new Scanner(System.in).next(); 
//		for(int i=0;i<40;i++){
//			System.out.println(i+" "+map2.size());
//			if(i<10){
//				getClustersFromHDFS("/import/portal/page_no_duplicate/duplicatePage-r-0000"+i+".avro");
//			}
//			else {
//				getClustersFromHDFS("/import/portal/page_no_duplicate/duplicatePage-r-000"+i+".avro");
//			}
//		}
//		Iterator<String>iterator=map2.keySet().iterator();
//		FileWriter writer1=new FileWriter("E://temp.txt");
//		while(iterator.hasNext()){
//			String hostStr=iterator.next();
//			writer1.write(hostStr+" "+map2.get(hostStr)+"\r\n");
//		}
//		writer1.close();
//		System.out.println("计算结束");
//		new Scanner(System.in).next();
		Configuration config = new Configuration();
		FileSystem hdfs = null;
		int coverSum=0,allUrlNum=0;
		try {
			hdfs = FileSystem.get(config);///import/portal/clear_page /work/ymyang2/clear_page_pattern2
			Path inputDir=new Path("/work/ymyang2/clear_page_pattern8");//garbage_pattern_type  _optimize1
			///parse/portal/parse_and_merge/2014-06-26-13-06
			FileStatus []inputFiles=hdfs.listStatus(inputDir);
			FileWriter writer=new FileWriter("E://1636.txt");
			int sum=0;
			if(inputFiles!=null){
				for (int i = 1; i < inputFiles.length; i++) {
					System.out.println(i);
					Path destFile=inputFiles[i].getPath();					
					try {
						InputStream is = hdfs.open(destFile);
						DataFileStream<GenericRecord> reader = new DataFileStream<GenericRecord>(
								is, new SpecificDatumReader<GenericRecord>());
						int cnt=0;
						while (reader.hasNext()) {
							GenericRecord item = reader.next();
							AvroKeyValue<Utf8,DownloadPage> data = new AvroKeyValue<Utf8,DownloadPage>(
									item);
							if(data.getValue().getIndexPage().getHost().toString().equals("home.babytree.com")){
								System.out.println(data.getValue().getUrl());
								if(sum>100000){
									writer.close();
									break;
								}
								writer.write(data.getValue().getUrl().toString());
							}
						}
						if(sum>100000){
							break;
						}
						IOUtils.cleanup(null, is);
						IOUtils.cleanup(null, reader);
					}catch(IOException ex) {
						ex.printStackTrace();
					}
				}			
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("end!!!!");
		new Scanner(System.in).next();
	}

	public static List<TreeNode> getTreeNodes(TreeNode rootNode) {
		List<TreeNode> nodeList = new ArrayList<TreeNode>();
		Queue<TreeNode> queue = new LinkedList<TreeNode>();
		queue.add(rootNode);
		TreeNode currentNode;
		while (!queue.isEmpty()) {
			currentNode = queue.poll();
			if (currentNode.getChildNodes() != null
					&& currentNode.getChildNodes().size() > 0) {
				for (TreeNode node : currentNode.getChildNodes()) {
					queue.add(node);
				}
			} else {
				nodeList.add(currentNode);
			}
		}
		return nodeList;
	}

	public static String getLeafTreeNodesPatterns(TreeNode rootNode) {
		String patternsStr = "";
		Queue<TreeNode> queue = new LinkedList<TreeNode>();
		queue.add(rootNode);
		TreeNode currentNode;
		boolean isFirst = true;
		while (!queue.isEmpty()) {
			currentNode = queue.poll();
			if (currentNode.getChildNodes() != null
					&& currentNode.getChildNodes().size() > 0) {
				boolean allChildIsLeafFlag = true;
				for (TreeNode treeNode : currentNode.getChildNodes()) {
					if (treeNode.getChildNodes() != null
							&& treeNode.getChildNodes().size() > 0) {
						allChildIsLeafFlag = false;
						break;
					}
				}
				if (currentNode.isChildHasStarFlag() && allChildIsLeafFlag) {
					queue.add(currentNode.getChildNodes().get(0));
					// System.out.println(currentNode.id+" "+currentNode.getPatternOutputStr());
					// System.out.println("end1:"+queue.size());
					continue;
				}
				for (TreeNode node : currentNode.getChildNodes()) {
					queue.add(node);
				}
			} else {
				String patternStr = "";
				for (char c : currentNode.getPatternOutputStr().toCharArray()) {
					if (c == '*') {
						patternStr += ".*";
					} else {
						patternStr += c;
					}
				}
				if (isFirst) {
					patternsStr += patternStr;
					isFirst = false;
				} else {
					patternsStr += "|" + patternStr;
				}
			}
		}
		return patternsStr;
	}

}
