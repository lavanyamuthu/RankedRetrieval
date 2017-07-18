import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
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

/* MAPPER INPUT - WORD:DOCID,wordsfreqindocid(number of times word occurs in doc),docfreqofword(number of docs word occurs)
 *                                                        d                      ,               n
 * MAPPER OUTPUT - <word1, docid1,wordsfreqindocid1,docfreqofword1>,<word1, docid2,wordsfreqindocid2,docfreqofword1>
 * OUTPUT - WORD=>DOCID,wordsfreqindocid(number of times word occurs in doc),docfreqofword1(number of docs word occurs),total number of words in DOCID
 *          WORD=>docid,                                      d,                                          n,                               D*/
public class RRStage2NumberofWordsinDoc extends Configured implements Tool{
	public static Map<String, String> treeMap = new TreeMap<String, String>();
	public static Map<String, String> worddocfreqMap = new TreeMap<String, String>();
	public static class RRStage2Mapper extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context)  throws IOException, InterruptedException {
			Text word = new Text();
			Text location = new Text();
			int count=0;
			String[] parse = key.toString().split(":");
			String temp= "";
			word.set(parse[0]);
			temp = parse[1]+","+value;
			location.set(temp);
			context.write(word, location);
			//System.out.println("word:"+word+" location:"+location);
			if(!treeMap.containsKey(parse[1])) {
				treeMap.put(parse[1],value.toString());
			}
			else {
				String test = treeMap.get(parse[1]);
				String[] test1 = test.split(",");
				String[] test2 = value.toString().split(",");
				count = Integer.parseInt(test1[0])+Integer.parseInt(test2[0]);
				//String combine = Integer.toString(count)+","+test1[1];
				String combine = Integer.toString(count);
				//System.out.println("docid:"+parse[1]+" count:"+count+" test2[1]:"+test2[1]+" test1[1]:"+test1[1]);
				//treeMap.put(parse[1], treeMap.get(parse[1]) + Integer.parseInt(value.toString()));
				treeMap.put(parse[1], combine);
				count=0;
				test1 = new String[2];
				test2 = new String[2];
			}
		}
	}
	public static class RRStage2Reducer1 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
			/*for(Map.Entry<String,String> entry : treeMap.entrySet()) {
				String value = entry.getValue();
				System.out.println(entry.getKey() + " => " + value);
			}*/
			Iterator itr = values.iterator();
			String token = " ", reducedval = "";
			String[] parse = new String[3];
			int count=0;
			while (itr.hasNext()) {
				//System.out.println("key:"+key+" token:"+token);
				token = itr.next().toString();
				parse = token.toString().split(",");
				//count = count+Integer.parseInt(parse[1]);
				//String[] test = treeMap.get(parse[0]).split(",");
				reducedval = token+","+treeMap.get(parse[0]);
				context.write(key, new Text(reducedval));
			}
			//System.out.println("o/p key:"+parse[0]+" content:"+key+":"+count);
			//System.out.println("key:"+key);
			//context.write(key, new Text("test"));
			
		}
	}
	private static String input = "data//output//RR//stage1//part-r-00000";
	private static String output = "data//output//RR//stage2";
	//private static String input = "data//output//RR//testoutputstage1";
	//private static String output = "data//output//RR//testoutputstage2";
	public final int run(String[] args) throws Exception { // NOPMD 
		  Configuration conf = getConf(); 
		  Job job = new Job(conf); 
		  job.setJobName("RRStage2NumberofWordsinDoc");  
		  job.setMapperClass(RRStage2Mapper.class); 
		  job.setReducerClass(RRStage2Reducer1.class); 
		  //job.setReducerClass(RRStage2Reducer2.class);
		  //job.setInputFormatClass(TextInputFormat.class);
		  job.setInputFormatClass(KeyValueTextInputFormat.class);
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
			res = ToolRunner.run(new RRStage2NumberofWordsinDoc(), args);
		} catch (Exception e) {
			e.printStackTrace();
		} 
		System.exit(res); 
	}
}