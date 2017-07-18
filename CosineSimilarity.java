import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CosineSimilarity extends Configured implements Tool {
	private static String query = "";
	public static HashMap<String,Integer> QueryWordsCount = new HashMap<String, Integer>();
	public static HashMap<String,Double> Querytf = new HashMap<String, Double>();
	public static HashMap<String,Double> Queryidf = new HashMap<String, Double>();
	public static HashMap<String,Double> Querytfidf = new HashMap<String, Double>();
	public static HashMap<String,Double> Doctf = new HashMap<String, Double>();
	public static HashMap<String,doctfidfdata> Wordtfidf = new HashMap<String, doctfidfdata>();
	public static HashMap<String,HashMap<String,doctfidfdata>> Doctfidf = new HashMap<String, HashMap<String,doctfidfdata>>();
	public static ArrayList<String> Docid = new ArrayList<String>();
	public static ArrayList<String> QueryWords = new ArrayList<String>();
	public static Map<String, Double> CosineSimilarity = new HashMap<String, Double>();
	public static class CosineMapper extends Mapper<LongWritable,Text,Text,Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (query.equals("")) {
				Configuration conf = context.getConfiguration();
				query = new String(conf.get("query"));
			}
			query = query.toLowerCase();
			String [] token = query.split(" ");
			Text word = new Text();
			Text location = new Text();
			String[] tokenize = value.toString().split("@");
			String[] parse = tokenize[1].split(",");
			String valword = tokenize[0].trim();
			String docid = parse[0];
			double d = Double.parseDouble(parse[1]);
			double D = Double.parseDouble(parse[2]);
			double n = Double.parseDouble(parse[3]);
			double tf = Double.parseDouble(parse[4]);
			double idf = Double.parseDouble(parse[5]);
			double tfidf = Double.parseDouble(parse[6]);
			
			for(int i=0;i<token.length;i++) {
				if(tokenize[0].trim().equals(token[i].trim())) {
					word.set(docid.trim());
					String temp1 = valword+","+Double.toString(d)+","+Double.toString(D)+","+Double.toString(n)+","+Double.toString(tf)+","+Double.toString(idf)+","+Double.toString(tfidf);
					location.set(temp1);
					context.write(word,location);
					if(!Queryidf.containsKey(token[i].trim())) {
						Queryidf.put(token[i].trim(),Double.parseDouble(parse[5]));
					}
				}
			}
		}	
	}
	
	public static class CosineReducer extends Reducer<Text,Text,Text,Text> {
		static String[] gileList = null;
	    static List<DataForRank> rList = new ArrayList<DataForRank>();
	    public static double cosineSimilarity(double[] docVector1,
				double[] docVector2) {
			double dotProduct = 0.0;
			double magnitude1 = 0.0;
			double magnitude2 = 0.0;
			double cosineSimilarity = 0.0;

			for (int i = 0; i < docVector1.length; i++) {
				dotProduct += docVector1[i] * docVector2[i];
				magnitude1 += Math.pow(docVector1[i], 2);
				magnitude2 += Math.pow(docVector2[i], 2);
			}

			magnitude1 = Math.sqrt(magnitude1);
			magnitude2 = Math.sqrt(magnitude2);

			if (magnitude1 != 0.0 | magnitude2 != 0.0) {
				cosineSimilarity = dotProduct / (magnitude1 * magnitude2);
			} else {
				return 0.0;
			}
			return cosineSimilarity;
		}


		public static void getCosineSimilarity(Context context) throws IOException, InterruptedException {
			if (query.equals("")) {
				Configuration conf = context.getConfiguration();
				query = new String(conf.get("query"));
			}
			query = query.toLowerCase();
			String [] token3 = query.split(" ");
			double[] vec1 = new double[Doctfidf.size()];
			double[] vec2 = new double[Doctfidf.size()];

			for (Map.Entry<String, HashMap<String, doctfidfdata>> entry : Doctfidf.entrySet()) {
				double num=0,denom1=0,denom2=0,sim=0;
				HashMap<String, doctfidfdata> temp1 = entry.getValue();
				//System.out.println("Hashmap of key:"+entry.getKey()+" size of value hash map:"+temp1.size());
				int i=-1;
				ArrayList<String> words = new ArrayList<String>();
				double[] doctfidf = new double[token3.length];
				double[] querytfidf = new double[token3.length];
				String docid = "";
				for (Map.Entry<String, doctfidfdata> entry1 : temp1.entrySet()) {
					i++;
					Text test = new Text(entry1.getKey()+entry1.getValue().tfidf+","+entry1.getValue().querytfidf);
					docid = entry.getKey();
					doctfidf[i] = entry1.getValue().tfidf;
					querytfidf[i] = entry1.getValue().querytfidf;
					words.add(entry1.getKey());
				}
				for(int k=0;k>QueryWords.size();k++) {
					if(! words.contains(QueryWords.get(k))) {
						i++;
						doctfidf[i] = 0;
						querytfidf[i] = Querytfidf.get(QueryWords.get(k));
					}
				}
				for(int j=0;j<doctfidf.length;j++) {
					num += (doctfidf[j]*querytfidf[i]);
					denom1 += doctfidf[j]*doctfidf[j];
					denom2 += querytfidf[i]*querytfidf[i];
				}
				sim = (num)/(Math.sqrt(denom1)*Math.sqrt(denom2));
				if(!CosineSimilarity.containsKey(docid)) {
					CosineSimilarity.put(docid,sim);
				}
			}
			
			if (query.equals("")) {
				Configuration conf = context.getConfiguration();
				query = new String(conf.get("query"));
			}
			query = query.toLowerCase();
			String [] token = query.split(" ");
			Set<String> files = null;
			int index = token.length;
			for(int i=0;i<token.length;i++) {
				if(Doctfidf.get(token[i])!=null){
					files = Doctfidf.get(token[i]).keySet();
					for(String file:files){
						gileList[index]=file;
						index++;
					}
				}
			}
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<String> wordlist = new ArrayList<String>();
			if (query.equals("")) {
				Configuration conf = context.getConfiguration();
				query = new String(conf.get("query"));
			}
			query = query.toLowerCase();
			String [] token = query.split(" ");
			for(int i=0;i<token.length;i++) {
				QueryWords.add(token[i]);
				if(!QueryWordsCount.containsKey(token[i])) {
					QueryWordsCount.put(token[i],1);
				}
				else {
					QueryWordsCount.put(token[i], QueryWordsCount.get(token[i] + 1));
				}
			}
			for (Map.Entry<String, Integer> entry : QueryWordsCount.entrySet()) {
			    if(!Querytf.containsKey(entry.getKey())) {
					Querytf.put(entry.getKey(),(double) ((double)entry.getValue()/(double)token.length));
				}
			}
			for(int i=0;i<token.length;i++) {
				if(!Querytfidf.containsKey(token[i])) {
					Querytfidf.put(token[i].trim(),Querytf.get(token[i])*Queryidf.get(token[i]));
				}
			}
			Iterator itr = values.iterator();
			String tokenize = " ", reducedval = "";
			double idf=0;
			double tf=0;
			int n=0;
			while (itr.hasNext()) {
				tokenize = itr.next().toString();
				String[] parse = tokenize.split(",");
				wordlist.add(parse[0].trim());
				if(!Doctfidf.containsKey(key.toString())) {
					Docid.add(parse[0].trim());
					HashMap<String,doctfidfdata> tfidfmap = new HashMap<String,doctfidfdata>();
					doctfidfdata temp = new doctfidfdata(Double.parseDouble(parse[6]),Querytfidf.get(parse[0].trim()));
					tfidfmap.put(parse[0].trim(), temp);
					Doctfidf.put(key.toString(), tfidfmap);	
				}
				else {
					HashMap<String,doctfidfdata> tfidfmap = new HashMap<String,doctfidfdata>();
					doctfidfdata temp = new doctfidfdata(Double.parseDouble(parse[6]),Querytfidf.get(parse[0].trim()));
					Doctfidf.get(key.toString()).put(parse[0].trim(), temp);
				}
			}
			
			for(int i=0;i<token.length;i++) {
				if(!wordlist.contains(token[i])) {
					HashMap<String,doctfidfdata> tfidfmap = new HashMap<String,doctfidfdata>();
					doctfidfdata temp = new doctfidfdata(0.0,Querytfidf.get(token[i].trim()));
					tfidfmap.put(token[i].trim(), temp);
					if(!Doctfidf.containsKey(key.toString())) {
						Doctfidf.put(key.toString(), tfidfmap);
					}
					else {
						HashMap<String,doctfidfdata> tfidfmap1 = new HashMap<String,doctfidfdata>();
						doctfidfdata temp1 = new doctfidfdata(0.0,Querytfidf.get(token[i].trim()));
						Doctfidf.get(key.toString()).put(token[i].trim(), temp);
					}
				}
			}
			for (Map.Entry<String, HashMap<String, doctfidfdata>> entry : Doctfidf.entrySet()) {
				HashMap<String, doctfidfdata> temp1 = entry.getValue();
				for (Map.Entry<String, doctfidfdata> entry1 : temp1.entrySet()) {
					Text test = new Text(entry1.getKey()+entry1.getValue().tfidf+","+entry1.getValue().querytfidf);
				}
			}
			getCosineSimilarity(context);
			Collections.sort(rList);
			for(DataForRank doc:rList){
			//context.write(new Text(doc.toString().split("::::::::::::")[0]), new Text(doc.toString().split("::::::::::::")[1]));
			}
		}
		protected void cleanup(Context context) throws IOException,InterruptedException {
			Map<String, Double> CosineSimilaritSort = new TreeMap<String, Double>(CosineSimilarity);
			Map sortedMap = sortByValue(CosineSimilarity);
			Iterator it = sortedMap.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        context.write(new Text(pair.getKey().toString()),new Text(pair.getValue().toString()));
		    }
		}
		public static Map sortByValue(Map unsortedMap) {
			Map sortedMap = new TreeMap(new ValueComparator(unsortedMap));
			sortedMap.putAll(unsortedMap);
			return sortedMap;
		}
	}

	private static final String OUTPUT_PATH = "data//output//queryop";
	//private static final String OUTPUT_PATH = "data//output//testqueryop";

	// where to read the data from.
	private static final String INPUT_PATH = "data//output//RR//stage3//part-r-00000";
	//private static final String INPUT_PATH = "data//output//RR//testoutputstage3//part-r-00000";

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		conf.set("query", args[2]);
		Job job = new Job(conf, "Final Phase");

		job.setMapperClass(CosineMapper.class);
		job.setReducerClass(CosineReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

		
		  //FileInputFormat.addInputPath(job, new Path(args[0]));
		  //FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		job.setJarByClass(CosineSimilarity.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String [] params = new String[3];
		int i =0;
		for(i=0;i<args.length;i++){
			params[i]=args[i];
		}
		FileReader file = new FileReader("data//extract//Query.txt");
		//FileReader file = new FileReader("data//extract//testquery.txt");

		//Configuration conf = new Configuration(); 
		
		BufferedReader br1 = new BufferedReader(file);
		
		params[i] = br1.readLine();
		//System.out.println("params length:"+params.length);
		//System.out.println(params[0]+params[1]+params[2]);
		int res = ToolRunner.run(new Configuration(), new CosineSimilarity(), params);
		//int res = ToolRunner.run(new SearchDist(), params);
	    //List<DataForRank> rList = new ArrayList<DataForRank>();
		
		System.exit(res);
	}
}
class doctfidfdata {
	//String word;
	//String docid;
	//int d;
	//int D;
	//int n;
	//int tf;
	//int idf;
	double tfidf;
	double querytfidf;
	doctfidfdata(double tfidf, double querytfidf) {
		this.tfidf = tfidf;
		//this.docid = docid;
		this.querytfidf = querytfidf;
	}
	public double gettfidf() {
		return this.tfidf;
	}
	public double getquerytfidf() {
		return this.querytfidf;
	}
}
class querytfidfdata {
	String word;
	//int docid;
	//int d;
	//int D;
	//int n;
	//int tf;
	//int idf;
	double tfidf;
	querytfidfdata(String word, double tfidf) {
		this.word = word;
		this.tfidf = tfidf;
	}
}
class DataForRank implements Comparable<DataForRank> {
	String docName;
	double tfidf;
	DataForRank(String doc, double tfidf){
		this.docName = doc;
		this.tfidf = tfidf;
	}
	public int compareTo(DataForRank rd) {
		int ret = 0;
		if (rd instanceof DataForRank) {
			if (this.tfidf == ((DataForRank) rd).tfidf) {
				ret = 0;
			} else
				ret = this.tfidf < ((DataForRank) rd).tfidf ? 1 : -1;
		}
		return ret;
	}
	public String toString(){
		return "Similarity = "+tfidf + "::::::::::::Doc= "+docName;
	}
}
class ValueComparator implements Comparator {
	Map map;
 
	public ValueComparator(Map map) {
		this.map = map;
	}
 
	public int compare(Object keyA, Object keyB) {
		Comparable valueA = (Comparable) map.get(keyA);
		Comparable valueB = (Comparable) map.get(keyB);
		return valueB.compareTo(valueA);
	}
}