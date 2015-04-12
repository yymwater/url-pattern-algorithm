package com.flytek.urlpattern.tree.applications;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flytek.urlpattern.tree.CalculateUtil;
import com.flytek.urlpattern.tree.Fun;
import com.flytek.urlpattern.tree.ProcessedUrl;
import com.flytek.urlpattern.tree.TreeNode;
import com.iflytek.common.HDFSFileUtils;
import com.iflytek.fs.avro.generate.DownloadPage;
import com.iflytek.fs.avro.generate.IndexPage;
import com.iflytek.gas.config.BaseConfig;
import com.iflytek.gas.config.MRConfiguration;
import com.iflytek.gas.counters.FSCounters;
import com.iflytek.gas.counters.TimeCounters;
import com.iflytek.parse.config.OdisLibConfig;
import com.iflytek.parse.html.commons.PageType;

public class PatternJob {
	private final static String NAME = "PATTERNJOB";
	private static final Logger log = LoggerFactory
			.getLogger(PatternJob.class);
	public static void main(String[] args) throws Exception {
//		Configuration conf = MRConfiguration.createUpdateQueqeConf();
		Configuration conf =  HBaseConfiguration.create();
		conf.set("mapred.job.queue.name","offline");
//		new TestJob().fun(conf);		// 设置小点,OOM
		conf.setFloat("mapred.job.shuffle.input.buffer.percent", 0.40f);
		// conf.set("mapred.child.java.opts", "-Xmx64768m");
		// 完成85% 才启动reduce
		conf.setFloat("mapred.reduce.slowstart.completed.maps", 0.99f);// map运行多少时，reduce启动
		conf.setInt("mapred.task.timeout", 0);// 不设置超时
		conf.set("mapreduce.reduce.memory.mb", "6144");// reduce的占内存设置
		conf.set("mapreduce.reduce.java.opts", "-Xmx3072m");

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("ERROR: Wrong number of parameters: "
					+ args.length);
			System.err.println("Usage: " + NAME
					+ " <inputpath> <outputpath> <reduce-num>");
			System.exit(-1);
		}
		int reduceNum = Integer.parseInt(otherArgs[2]);
		log.info(NAME + " : starting");
		Job job = CreateParseJob(conf, otherArgs[0], otherArgs[1], reduceNum);
		boolean success = job.waitForCompletion(true);
		if (success) {
			log.info(NAME + " : done.");
		} else {
			log.info(NAME + " : failed");
		}
	}

	public static Job CreateParseJob(Configuration conf, String inpath,
			String outpath, int reduceNum) {
		try {
			Job job = new Job(conf);
			job.setJarByClass(PatternJob.class);
			job.setJobName(NAME + "_" + inpath + "_" + outpath);

			HDFSFileUtils.removeFile(outpath);
			// output.getFileSystem(job).delete(output, true);

			job.setMapperClass(UrlStateMergeMap.class);
			job.setReducerClass(UrlStateMergeReduce.class);

			// 输入输出文件格式
			// job.setOutputFormatClass(AvroKeyOutputFormat.class);
			job.setInputFormatClass(AvroKeyValueInputFormat.class);
			job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
			
//			job.setOutputKeyClass(Text.class);
//			job.setOutputValueClass(Text.class);
			String[] pathsStrings = inpath.split("\\+");
			for (String path : pathsStrings) {
				log.info("add input path " + path);
				AvroKeyValueInputFormat.addInputPath(job, new Path(path));
			}

			FileOutputFormat.setOutputPath(job, new Path(outpath));

			// 设置Schema
			// AvroJob.setInputSchema(job, IndexPage.SCHEMA$);
			AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
			AvroJob.setInputValueSchema(job, DownloadPage.SCHEMA$);
			AvroJob.setMapOutputKeySchema(job,
					Schema.create(Schema.Type.STRING));
			AvroJob.setMapOutputValueSchema(job,Schema.create(Schema.Type.STRING));

			AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
			AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.STRING));

			job.setNumReduceTasks(reduceNum);

			return job;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 
	 * @author "hpma@iflytek.com"
	 * 
	 */
	static class UrlStateMergeMap
			extends
			Mapper<AvroKey<String>, AvroValue<DownloadPage>, AvroKey<String>, AvroValue<String>> {
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}

		@Override
		protected void map(AvroKey<String> key, AvroValue<DownloadPage> value,
				Context context) throws IOException, InterruptedException {
			context.getCounter(FSCounters.Rows).increment(1);
			try {
				// 按domain来排序
				DownloadPage page = value.datum();
				IndexPage ipage = page.getIndexPage();
//				if (ipage.getType().toString().equals("THEME_PAGE")) {
					String host = page.getIndexPage().getHost().toString();
					String url = page.getRawUrl().toString();
					context.write(new AvroKey<String>(host),
						new AvroValue<String>(url));
//				}
				
			} catch (Exception e) {
				context.getCounter(FSCounters.Error).increment(1);
				e.printStackTrace();
			}
		}
	}

	/**
	 * 
	 * 
	 * @author "hpma@iflytek.com"
	 * 
	 */
	static class UrlStateMergeReduce
			extends
			Reducer<AvroKey<String>, AvroValue<String>,AvroKey<String>,AvroValue<String>> {
		private String fileNameStr = "/work/ymyang2/clear_page_prepare";
		private final HashMap <String,String> map=new HashMap<String,String>();
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			//从HDFS文件中进行加载pattern
			Configuration config = new Configuration();
			FileSystem hdfs = null;
			try {
				hdfs = FileSystem.get(config);
				Path inputDir=new Path(fileNameStr);
				FileStatus []inputFiles=hdfs.listStatus(inputDir);
				if(inputFiles!=null){
					for (int i = 1; i < inputFiles.length; i++) {
						Path destFile=inputFiles[i].getPath();					
						try {
							InputStream is = hdfs.open(destFile);
							DataFileStream<GenericRecord> reader = new DataFileStream<GenericRecord>(
									is, new SpecificDatumReader<GenericRecord>());
							while (reader.hasNext()) {
								GenericRecord item = reader.next();
								AvroKeyValue<Utf8,Utf8> data = new AvroKeyValue<Utf8, Utf8>(
										item);
								map.put(data.getKey().toString(),data.getValue().toString());
							}
							IOUtils.cleanup(null, is);
							IOUtils.cleanup(null, reader);
						}catch(IOException ex) {
							ex.printStackTrace();
						}
					}		
				}
				//在离线阶段有10个reducer在运行，则有10个结果文件
			} catch (IOException e) {
				e.printStackTrace();
			}
			BaseConfig.getInstance().loadConfFromJobConf(
					context.getConfiguration());
			super.setup(context);
		}
		@Override
		protected void reduce(AvroKey<String> _key,
				Iterable<AvroValue<String>> values, Context context)
				throws IOException, InterruptedException {
			String host = new Utf8(_key.toString()).toString();
			int allUrlNum=0,trainUrlNum=0;
			long start = System.nanoTime();
			try {
				List<ProcessedUrl> urlsList = new ArrayList<ProcessedUrl>();
				int sum=0;
				if(map.get(host)!=null){
					sum=Integer.parseInt(map.get(host));
				}
				Random random=new Random();
				for (AvroValue<String> item : values) {
					allUrlNum++;
					if(sum>500000&&random.nextDouble()>500000.0/sum)
						continue;
					trainUrlNum++;
					String url = item.datum();
					ProcessedUrl processedUrl = new ProcessedUrl(url);
					urlsList.add(processedUrl);
				}
				if(urlsList.size() < 100000) {// 如果该domain下themePage很少则退出
					return;
				}
				log.info("the host is\t" + host + "\t"
						+ "the theme_page counts is\t" + urlsList.size());
				List<String> keyDoneList = new ArrayList<String>();
				Fun fun=new Fun();
				TreeNode rootNodeOfPatternTree = fun.buildPatternTree(urlsList,
						keyDoneList, null, null);
				String resultStr=getLeafTreeNodesPatterns(rootNodeOfPatternTree);
				if(urlsList.size()>10000){
					log.info("###"+host+" "+resultStr);
				}
				if(resultStr!=null&&resultStr.length()>0){					
					context.write(new AvroKey<String>(host),new AvroValue<String>("#"+allUrlNum+"$"+trainUrlNum));					
					context.write(new AvroKey<String>(host),new AvroValue<String>(":"+resultStr));					
				}
				context.getCounter(TimeCounters.UrlPattern)
						.increment(
								TimeUnit.NANOSECONDS.toMillis(System.nanoTime()
										- start));
			} catch (Exception e) {
				context.getCounter(FSCounters.Error).increment(1);
				e.printStackTrace();
			}
		}
		public static String getLeafTreeNodesPatterns(TreeNode rootNode){
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
//					String patternStr="";
					String patternStr=currentNode.getPatternOutputStr();
//					for(char c:currentNode.getPatternOutputStr().toCharArray()){
//						if(c=='*'){
//							patternStr+=".*";
//						}else{
//							patternStr+=c;
//						}
//					}
					int displayNum=currentNode.getUrlsList().size();
					if(displayNum>300)
						displayNum=300;
					String urlStr=currentNode.getUrlsList().size()+"$"+displayNum+"$"+currentNode.getUrlsList().get(0).getUrlStr();
					for(int i=1;i<currentNode.getUrlsList().size()&&i<300;i++){
						if(currentNode.getUrlsList().get(i).getUrlStr().length()<300)
							urlStr+="$"+currentNode.getUrlsList().get(i).getUrlStr();
					}
					if(isFirst){
						patternsStr+=patternStr+"$"+urlStr;
						isFirst=false;
					}else{
						patternsStr+="|"+patternStr+"$"+urlStr;
					}
				}
			}
			return patternsStr;
		}
	}
}
