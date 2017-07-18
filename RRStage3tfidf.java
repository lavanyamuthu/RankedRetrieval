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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* MAPPER INPUT - WORD=>DOCID,wordsfreqindocid(number of times word occurs in doc),docfreqofword1(number of docs word occurs),total number of words in DOCID
 *                WORD=>docid,                                      d,                                          n,                               D
 * MAPPER OUTPUT - WORD=>DOCID,wordsfreqindocid(number of times word occurs in doc),docfreqofword1(number of docs word occurs),total number of words in DOCID
 *                 WORD=>docid,                                     d,                                          n,                               D
 * OUTPUT - WORD @DOCID:freq of word in doc(d):total number of words in doc(D):total number of docs word appears (n),
 * tf(d/D),idf(1 + loge(Total Number Of Documents / Number Of Documents with term game in it),tf*idf*/
public class RRStage3tfidf extends Configured implements Tool{
	public static int N = 22878;
	//public static double N = 3;
	//public static Map<String, Integer> treeMap = new TreeMap<String, Integer>();
	public static class RRStage3Mapper extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			Text word = new Text();
			Text location = new Text();
			//String[] parse = key.toString().split("\t");
			String temp= "";
			//System.out.println("key:"+key+" value:"+value);
			word.set(key);
			//temp = parse[1]+":"+value;
			location.set(value);
			context.write(word, location);
			//System.out.println("word:"+word+" location:"+location);
			/*if(!treeMap.containsKey(key.toString())) {
				treeMap.put(key.toString(),1);
			}
			else {
				treeMap.put(key.toString(), treeMap.get(key.toString()) + 1);
			}*/
		}
	}
	public static class RRStage3Reducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator itr = values.iterator();
			String token = " ", reducedval = "";
			//String[] parse = new String[2];
			double idf=0;
			double tf=0,n=0;
			int count=0;
			//System.out.println("next key is:"+key);
			while (itr.hasNext()) {
				token = itr.next().toString();
				String[] parse  = token.split(",");
				//System.out.println("key:"+key+" token:"+token);
				//tf = 1+(double) Math.log10(Double.parseDouble(parse[1])/Double.parseDouble(parse[2]));
				tf = Double.parseDouble(parse[1])/Double.parseDouble(parse[3]);
				//tf = 1+(double) Math.log10(Double.parseDouble(parse[1]));
				//n = treeMap.get(key.toString());
				n = Integer.parseInt(parse[2]);
				//idf = Math.log10(N/n);
				idf = 1+Math.log(N/n);
				//System.out.println("idf of "+key+" is:"+idf+" N is:"+N+" n is:"+n+" log is:"+Math.log(N/n));
				//reducedval = "@"+token+":"+treeMap.get(key.toString())+","+tf+","+idf+","+(tf*idf);
				reducedval = "@"+parse[0]+","+parse[1]+","+parse[3]+","+parse[2]+","+tf+","+idf+","+(tf*idf);
				//reducedval = token+":"+treeMap.get(key.toString());
				context.write(key, new Text(reducedval));
				count++;
			}
			
		}
	}
	private static String input = "data//output//RR//stage2//part-r-00000";
	//private static String input = "data//output//RR//testoutputstage2//part-r-00000";
	private static String output = "data//output//RR//stage3";
	//private static String output = "data//output//RR//testoutputstage3";
	public final int run(String[] args) throws Exception { // NOPMD 
		  Configuration conf = getConf(); 
		  Job job = new Job(conf); 
		  job.setJobName("RRStage3tfidf");  
		  job.setMapperClass(RRStage3Mapper.class); 
		  job.setReducerClass(RRStage3Reducer.class); 
		  job.setInputFormatClass(KeyValueTextInputFormat.class);
		  job.setOutputFormatClass(TextOutputFormat.class);
		  job.setOutputKeyClass(Text.class); 
		  job.setOutputValueClass(Text.class);
		  FileInputFormat.addInputPath(job, new Path(input)); 
		  FileOutputFormat.setOutputPath(job, new Path(output));
		  System.exit(job.waitForCompletion(true) ? 0 : 1); 
		  return 0; 
	} 
	public static void main(String[] args) {
		int res = 0;
		try {
			res = ToolRunner.run(new RRStage3tfidf(), args);
		} catch (Exception e) {
			e.printStackTrace();
		} 
		System.exit(res); 
	}
}