import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* MAPPER INPUT - zipfile reader
 * MAPPER OUTPUT/REDUCER INPUT - (<word1>,<docid1>),(<word1,docid2>),(<word1,docid3>)
 * REDUCER OUTPUT - WORD:DOCID,wordsfreqindocid(number of times word occurs in doc),docfreqofword(number of docs word occurs)*/
public class RRStage1WordsFrequency extends Configured implements Tool{
	public static class RRStage1Mapper extends Mapper<Text, BytesWritable, Text, Text> {
		public void map(Text key, BytesWritable content, Context context)  throws IOException, InterruptedException {
			Text word = new Text();
			Text location = new Text();
			//IntWritable location = new IntWritable(1);
			ReutersDoc FileContent = StringUtils.getXMLContent(content);
			String extractedContents = StringUtils.normalizeText(FileContent.getContent());
			location.set(FileContent.getDocID());
			String[] temp = extractedContents.split(" ");
			for(int i=0;i<temp.length;i++) {
				if(!temp[i].equals(" ") && !temp[i].equals("a") && !temp[i].equals("aa") && !temp[i].equals("aaa") 
					&& !temp[i].equals("") && !temp[i].equals("aaaaaa") && !temp[i].equals("aaabaa") && !temp[i].equals("aaabbbplus") 
					&& !temp[i].equals("aaafaaa") && !"".equals(temp[i].trim()) && !temp[i].equals("of")) {
					//String test = temp[i]+":"+FileContent.getDocID();
					String temp1 = location+":"+Integer.toString(1); 
					location.set(temp1);
					word.set(temp[i]);
					context.write(word, location);
					//System.out.println("word:"+word+" location:"+location);
				}
			}
		}
	}
	public static class RRStage1Reducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
			//System.out.println("In reducer");
			//ArrayList<String> a2 = new ArrayList<String>();
			Map<String, Integer> treeMap = new TreeMap<String, Integer>();
			Map<String, Integer> docFreqMap = new TreeMap<String, Integer>();
			Iterator itr = values.iterator();
			String token = " ";
			String[] parse = new String[2];
			int count=0;
			//StringBuilder sb = new StringBuilder();
			//token = itr.next().toString();
			while (itr.hasNext()) {
				token = itr.next().toString();
				parse = token.split(":");
				//System.out.println("parse[0]:"+parse[0]);
				//System.out.println("parse[1]:"+parse[1]);
				if(!treeMap.containsKey(parse[0])) {
					//sb.append(token);
				    //sb.append(",");
					treeMap.put(parse[0],Integer.parseInt(parse[1]));
					count++;
				}
				else {
					//System.out.println("treeMap.get(parse[0]):"+treeMap.get(parse[0]));
					//treeMap.put(parse[0], treeMap.get(key) + Integer.parseInt(parse[1]));
					treeMap.put(parse[0], treeMap.get(parse[0]) + 1);
				}
				/*if(!docFreqMap.containsKey(key+":"+parse[0])) {
					//sb.append(token);
				    //sb.append(",");
					docFreqMap.put((key+":"+parse[0]),Integer.parseInt(parse[1]));
				}
				else {
					//System.out.println("treeMap.get(parse[0]):"+treeMap.get(parse[0]));
					//treeMap.put(parse[0], treeMap.get(key) + Integer.parseInt(parse[1]));
					docFreqMap.put((key+":"+parse[0]), docFreqMap.get(parse[0]) + 1);
				}*/
			}
			//System.out.println("word:"+key+" count:"+count);
			for(Map.Entry<String,Integer> entry : treeMap.entrySet()) {
				  String reducerKey = key+":"+entry.getKey();
				  Integer value = entry.getValue();
				  Text test = new Text(value.toString()+","+count);
				  //context.write(new Text(reducerKey),new IntWritable(value));
				  context.write(new Text(reducerKey),test);
				  //System.out.println(key+ "=>" + entry.getKey() + " => " + value);
			}
			count=0;
		}
	}
	private static String input = "data//txt1";
	private static String output = "data//output//RR//stage1";
	public final int run(String[] args) throws Exception { // NOPMD 
		  Configuration conf = getConf(); 
		  Job job = new Job(conf); 
		  job.setJobName("RRStage1WordsFrequency"); 
		  job.setMapperClass(RRStage1Mapper.class); 
		  job.setReducerClass(RRStage1Reducer.class); 
		  job.setInputFormatClass(ZipFileInputFormat.class);
		  job.setOutputFormatClass(TextOutputFormat.class);
		  job.setOutputKeyClass(Text.class); 
		  job.setOutputValueClass(Text.class);
		  FileInputFormat.addInputPath(job, new Path(input)); 
		  FileOutputFormat.setOutputPath(job, new Path(output));
		  //FileInputFormat.addInputPath(job, new Path(args[0])); 
		  //FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  System.exit(job.waitForCompletion(true) ? 0 : 1); 
		  return 0; 
	} 
	public static void main(String[] args) {
		int res = 0;
		try {
			res = ToolRunner.run(new RRStage1WordsFrequency(), args);
		} catch (Exception e) {
			e.printStackTrace();
		} 
		System.exit(res); 
	}
}