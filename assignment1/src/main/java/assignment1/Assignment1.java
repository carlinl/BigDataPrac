package assignment1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;//split the inputfile

/**
 * 
 * This class solves the problem posed for Assignment1
 *
 */
class Nword{
	String [] words; //to store the text file
	int n; // ngram	
	int p = 0; //like a pointer in the list
	public Nword(String value, int ngram) {
		this.words = value.split(" ");
		this.n = ngram;
	}
	//to check does the words list meet the end 
	public boolean hasNext() {
		if (p < words.length - n + 1) {
			return true;
		}
		else {
			return false;
		}
	}
	//read next n words
	public String nextTuple() {
		StringBuffer sb = new StringBuffer();
		for(int i = 0; i < n; i++) {
			if (i == 0) {
				sb.append(words[i+p]);
			}
			else {
				sb.append(" "+words[i+p]);
			}	
		}
		this.p = p + n;
		return sb.toString();
	}
	
}
class outTuple implements Writable{
	private IntWritable one;
	private Text fname;
	
	
	public outTuple() {
		this.one = new IntWritable(0);
		this.fname = new Text();
	}
	public outTuple(IntWritable w, Text t) {
		this.one = w;
		this.fname = t;
	}
	public void setOne(IntWritable w) {
		this.one = w;
	}
	public void setFname(Text t) {
		this.fname = t;
	}
	public void set(IntWritable w, Text t) {
		this.one = w;
		this.fname = t;
	}
	public IntWritable getOne() {
		return one;
	}
	public Text getFname() {
		return fname;
	}
	public void readFields(DataInput in) throws IOException {

		one.readFields(in);
		fname.readFields(in);
	}
	public void write(DataOutput out) throws IOException {

		one.write(out);
		fname.write(out);
	}
	public String toString() {
		return one.get() + " " + fname.toString();
	}
}

public class Assignment1 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, outTuple> {
		//input is filekey and file
		//out put is (word,pair)
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);
		
		
		private int ngram;
		private Text fname = new Text();
		
		public void map(Object key, Text value, Context context)throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			ngram = Integer.parseInt(conf.get("ngram"));
			FileSplit fs = (FileSplit) context.getInputSplit();
			fname.set(fs.getPath().getName());
			
			Nword itr = new Nword(value.toString(),ngram);
			while(itr.hasNext()) {
				word.set(itr.nextTuple());
				context.write(word, new outTuple(one, fname));
			}
		}
		
	}

	public static class IntSumReducer extends Reducer<Text, outTuple, Text, outTuple> {
		private outTuple result = new outTuple();
		
		public void reduce(Text key, Iterable<outTuple> values, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			
			int min_count = Integer.parseInt(conf.get("min_count"));
			int sum = 0;
			
			Set<String> fb = new HashSet<String>();
			for(outTuple val:values) {
				sum += val.getOne().get();
				StringTokenizer itr = new StringTokenizer(val.getFname().toString(),":");
				
				while(itr.hasMoreTokens()) {
					fb.add(itr.nextToken());
				}
			
			if (sum < min_count) return;
				
			StringBuffer sb = new StringBuffer();
			Iterator<String> fitr = fb.iterator();
			
			while(fitr.hasNext()) {
				sb.append((sb.length() == 0 ? "" : ":") + fitr.next());
			}
					
			result.set(new IntWritable(sum), new Text(sb.toString()));
			context.write(key, result);
		}
	}
}

	public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		    String [] remain = optionParser.getRemainingArgs();
		    conf.set("ngram", remain[0]);
		    conf.set("min_count", remain[1]);
		    
		    Job job = Job.getInstance(conf, "assignment1");
		    
		    job.setJarByClass(Assignment1.class);
		    
		    
		    job.setMapperClass(TokenizerMapper.class);
		    //job.setCombinerClass(IntSumReducer.class);
		    job.setReducerClass(IntSumReducer.class);
		    
		    //job.setNumReduceTasks(0);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(outTuple.class);
		    
		    
		    
		    FileInputFormat.addInputPath(job, new Path(args[2]));
		    FileOutputFormat.setOutputPath(job, new Path(args[3]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
		
}



 