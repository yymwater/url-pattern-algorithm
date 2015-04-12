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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
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
import com.iflytek.gas.pipeline.PageTypeFixedPipeline;
import com.iflytek.parse.config.OdisLibConfig;
import com.iflytek.parse.html.commons.PageType;
import com.sun.corba.se.impl.oa.poa.ActiveObjectMap.Key;
import com.sun.org.apache.bcel.internal.generic.NEW;

public class TestJob3 {
	private final static String NAME = "TestJob2";
	private static final Logger log = LoggerFactory
			.getLogger(TestJob3.class);

	public static void main(String[] args) throws Exception {
		Configuration conf =  HBaseConfiguration.create();
		conf.set("mapred.job.queue.name","offline");
		// 设置小点,OOM
		conf.setFloat("mapred.job.shuffle.input.buffer.percent", 0.40f);
		// conf.set("mapred.child.java.opts", "-Xmx64768m");
		// 完成85% 才启动reduce
		conf.setFloat("mapred.reduce.slowstart.completed.maps", 0.99f);// map运行多少时，reduce启动
		conf.setInt("mapred.task.timeout", 0);// 不设置超时
		conf.set("mapreduce.reduce.memory.mb", "12288");// reduce的占内存设置
		conf.set("mapreduce.reduce.java.opts", "-Xmx10240m");

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
			job.setJarByClass(TestJob3.class);
			job.setJobName(NAME + "_" + inpath + "_" + outpath);

			HDFSFileUtils.removeFile(outpath);

			job.setMapperClass(UrlStateMergeMap.class);
			job.setReducerClass(UrlStateMergeReduce.class);

			// 输入输出文件格式
//			job.setInputFormatClass(AvroKeyValueInputFormat.class);
//			job.setOutputKeyClass(Text.class);
//			job.setOutputValueClass(Text.class);

//			String[] pathsStrings = inpath.split("\\+");
//			for (String path : pathsStrings) {
//				log.info("add input path " + path);
//				AvroKeyValueInputFormat.addInputPath(job, new Path(path));
//			}
			
//			FileOutputFormat.setOutputPath(job, new Path(outpath));
			job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(inpath));
		    FileOutputFormat.setOutputPath(job, new Path(outpath));
			// 设置Schema
//			AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
//			AvroJob.setInputValueSchema(job, DownloadPage.SCHEMA$);
//			AvroJob.setMapOutputKeySchema(job,
//					Schema.create(Schema.Type.STRING));
//			AvroJob.setMapOutputValueSchema(job, DownloadPage.SCHEMA$);
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
			Mapper<Object, Text, Text, Text> {
		public HashMap <String,String>map=new HashMap<String,String>();
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}

		@Override
		protected void map(Object key, Text value,
				Context context) throws IOException, InterruptedException {
			context.getCounter(FSCounters.Rows).increment(1);
//			DownloadPage page = value.datum();
//			if(map.get(page.getUrl().toString())==null){
//				return;
//			}
			try {
				// 按domain来排序
				String url=value.toString();
				String host =null;
				if(url.contains("bbs.sina.cn")){
					host="bbs.sina.cn";
				}
				if(url.contains("m.58.com")){
					host="m.58.com";
				}
				if(url.contains("3g.yuehui.163.com")){
					host="3g.yuehui.163.com";
				}
				if(url.contains("cq.3g.cn")){
					host="cq.3g.cn";					
				}
				if(url.contains("m.nuomi.com")){
					host="m.nuomi.com";					
				}
				if(url.contains("news.sina.cn")){
					host="news.sina.cn";			
				}
				context.write(new Text(host),new Text(url.split(" ")[0]));

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
			Reducer<Text, Text, Text,Text> {

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			BaseConfig.getInstance().loadConfFromJobConf(
					context.getConfiguration());
			super.setup(context);
		}
		@Override
		protected void reduce(Text key,
				Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			PageTypeFixedPipeline judgeTool=new PageTypeFixedPipeline();
			for(Text page:values){
				String urlStr=page.toString();
				Text text=new Text();
				String flag=judgeTool.IsThemePage(urlStr);
				log.info(urlStr+" "+flag);
				if(flag.equals("THEME_PAGE")){
					text.set("1");
					log.info("##"+key.toString()+" "+PageTypeFixedPipeline.map.get(new Utf8(key.toString()))+" "+CalculateUtil.getSortedUrl(urlStr));
					context.write(new Text(urlStr),text); 
				}
				else{
					text.set("0");
					log.info("##"+key.toString()+" "+PageTypeFixedPipeline.map.get(new Utf8(key.toString()))+" "+CalculateUtil.getSortedUrl(urlStr));
					context.write(new Text(urlStr),text);
				}
			}
		}
	}
}
