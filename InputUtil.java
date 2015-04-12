package com.flytek.urlpattern.tree;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.iflytek.common.Console;
import com.iflytek.fs.avro.generate.DownloadPage;
import com.iflytek.fs.avro.generate.DuplicatePages;
import com.iflytek.fs.avro.generate.IndexPage;
import com.iflytek.fs.avro.generate.Link;
import com.iflytek.parse.html.analyzers.Url;
import com.iflytek.trustrank.graph.AdjacencyGraph;
import com.iflytek.trustrank.graph.DirectedAdjacencyGraph;
import com.iflytek.trustrank.pagerank.PageRankRunner;
import com.iflytek.trustrank.trustrank.InversePageRankSeedSelectionStrategy;
import com.iflytek.trustrank.trustrank.TrustRankRunner;
/**
 * author:Yang Yiming
 * 用于从文件中读取url的工具类
 * date:2014.05.23
 * **/
public class InputUtil {
	//从HDFS中 读取avro数据然后存到本地
	public static void getUrlFromHDFS(String fileName){
		Configuration config = new Configuration();
		FileSystem hdfs = null;
		try {
//			BufferedWriter bw = new BufferedWriter(new FileWriter("E:/theme_page.txt"));
			hdfs = FileSystem.get(config);
			Path destFile = new Path(fileName);
			try {
				InputStream is = hdfs.open(destFile);
				DataFileStream<GenericRecord> reader = new DataFileStream<GenericRecord>(
						is, new SpecificDatumReader<GenericRecord>());	
				int sum=0;
				while(reader.hasNext()) {
					GenericRecord item = reader.next();
					AvroKeyValue<String, DownloadPage> data = new AvroKeyValue<String, DownloadPage>(item);
					DownloadPage dp = data.getValue();
					if(dp.getIndexPage().getType().toString().equals("THEME_PAGE")){						
						String urlStr=dp.getUrl().toString();				
//						bw.write(urlStr);bw.newLine();
//						bw.flush();
					};
				}
//				bw.close();
				System.out.println(sum);
				IOUtils.cleanup(null, is);
				IOUtils.cleanup(null, reader);
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	//从文件中读取URL：该fileName文件中有URL和keyValuePair(文件格式：每一个URL和其每一个KeyValuePair均占一行)
	public static List<ProcessedUrl>getUrlsFromFile(String fileName){
		List <ProcessedUrl> urlsList=new ArrayList<ProcessedUrl>();
		try {
			FileReader fileReader=new FileReader(fileName);
			BufferedReader reader=new BufferedReader(fileReader);
			String lineStr,part[];
			ProcessedUrl processedUrl=null;
			List<KeyValuePair>pairList=null;
			while((lineStr=reader.readLine())!=null){
				if(lineStr.startsWith("/")||lineStr.startsWith(" ")){
					if((processedUrl!=null)||lineStr.startsWith(" ")){
						processedUrl.setKeyValuePairs(pairList);
						urlsList.add(processedUrl);
					}
					processedUrl=new ProcessedUrl();
					processedUrl.setUrlStr(lineStr);
					pairList=new ArrayList<KeyValuePair>();
				}else {
					part=lineStr.split(" ");
					pairList.add(new KeyValuePair(part[0],part[1]));
				}
			}
		} catch (FileNotFoundException e) {
			System.out.println("文件"+fileName+"未找到！");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("读取文件出现！");
			e.printStackTrace();
		}
		return urlsList;
	}
	//从文件中读取URL：该fileName文件中只有URL(文件格式：一条URL占一行)
	public static List<ProcessedUrl>getUrlsFromUrlFile(String fileName){
		List <ProcessedUrl> urlsList=new ArrayList<ProcessedUrl>();
		try {
			FileReader fileReader=new FileReader(fileName);
			BufferedReader reader=new BufferedReader(fileReader);
			String lineStr;
			ProcessedUrl processedUrl=null;
			while((lineStr=reader.readLine())!=null){
				urlsList.add(new ProcessedUrl(lineStr));
			}
		} catch (FileNotFoundException e) {
			System.out.println("文件"+fileName+"未找到！");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("读取文件出现！");
			e.printStackTrace();
		}
		return urlsList;
	}
	//从文件中读取clusters
	public static void getClustersFromHDFS(String fileName,int clusterNum){
		Configuration config = new Configuration();
		FileSystem hdfs = null;
		try {
			hdfs = FileSystem.get(config);
			Path destFile = new Path(fileName);
			InputStream is =hdfs.open(destFile);
			DataFileStream<DuplicatePages> reader = new DataFileStream<DuplicatePages>(is,
					new SpecificDatumReader<DuplicatePages>());
			long sum=0;
			BufferedWriter bw = new BufferedWriter(new FileWriter("E:/clear_page.txt"));
			while (reader.hasNext()) {
				DuplicatePages pages = reader.next();
				List<CharSequence>urlsCharSequences=pages.getUrls();
				System.out.println(urlsCharSequences.size());
				if(urlsCharSequences.size()>1000){
					continue;
				}
//				bw.write("#");bw.newLine();
				sum++;
				if(sum>clusterNum){
					break;
				}
				for(CharSequence sequence:urlsCharSequences){
					System.out.println(sequence.toString());
					bw.write(sequence.toString());bw.newLine();
					bw.flush();
				}
			}
			bw.close();
			IOUtils.cleanup(null, is);
			IOUtils.cleanup(null, reader);
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	public static List<Cluster>getClustersFromFile(String fileName) throws IOException{
		List<Cluster>clustersList=new ArrayList<Cluster>();
		BufferedReader reader=new BufferedReader(new FileReader(fileName));
		String oneLineDataStr;
		Cluster cluster=null;
		while((oneLineDataStr=reader.readLine())!=null){
			if(oneLineDataStr.equals("#")){
				if(cluster!=null){
					if(cluster.getUrlList().size()>10)
						clustersList.add(cluster);
				}
				cluster=new Cluster();
			}else{
				cluster.add(oneLineDataStr);
			}
		}
		return clustersList;
	}
	//从配置文件中读取信息
	public static String readProperties(String key){
		File file=new File("urlpattern/urlPatternTree.properties");
		if(!file.exists()){
			System.out.println("配置文件未找到");
			return "1";
		}else{
			try {
				FileInputStream fis=new FileInputStream(file);
				Properties propertiesFile=new Properties();
				propertiesFile.load(fis);
				fis.close();
				return propertiesFile.getProperty(key);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	public static void main(String argos[]){
//		getUrlFromHDFS("/import/portal/page/part-r-00001.avro");
//		getClustersFromHDFS("/import/portal/page_no_duplicate/duplicatePage-r-00001.avro",10000);
		getUrlFromHDFS("/import/portal/clear_page/part-r-00001.avro");
	}
}
