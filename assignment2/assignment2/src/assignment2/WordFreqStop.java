package assignment2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordFreqStop {	
		
	/*
	 * WordFreq Job
	 */
	public static class WordFreqStopMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		public HashSet<String> stopWords = new HashSet<String>();
		BufferedReader br;
		
		protected void setup(Context context) throws IOException, InterruptedException{
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());   	
	        br = new BufferedReader(new FileReader(cacheFiles[0].toString()));
	        String line;
	        while ((line = br.readLine()) != null){
	                stopWords.add(line);
	        }
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			line = line.replaceAll(",", " ");
			line = line.replaceAll("-", " ");
			line = line.replaceAll("\\.", " ");
			line = line.toLowerCase();
			
			StringTokenizer tokenizer = new StringTokenizer(line);
			tokenizer.nextToken();
			while(tokenizer.hasMoreTokens()){
				String word = tokenizer.nextToken();
				if(!stopWords.contains(word)){
					context.write(new Text(word), new IntWritable(1));
				}
			}
		}
	}
	
	public static class WordFreqStopReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
				
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			IntWritable result = new IntWritable(0);
			for (IntWritable val : values) {
				result.set(result.get() + val.get());
			}
			context.write(key, result);		
		}
	}

	/*
	 * Sorting Job
	 */
	public static class WordFreqStopSortMapper extends Mapper<LongWritable, Text, IntegerWritable , Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String[] line = value.toString().split("\t");
			Text word = new Text(line[0]);

			IntegerWritable hits = new IntegerWritable(Integer.parseInt(line[1]));
			context.write(hits, word);
		}
	}
	
	public static class WordFreqStopSortReducer extends Reducer<IntegerWritable, Text , Text , IntegerWritable>{
			
		@Override
		protected void reduce(IntegerWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for(Text i : values){
				context.write(i,key);
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		
		final String input_path		= args[0]+"data/plot_summaries.txt";
		final String temp_path 		= args[0]+"temp/";
		final String output_path	= args[0]+args[1];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		/*
		 * WordFreqStop Job
		 */
		
		if (fs.exists(new Path (temp_path))){
			fs.delete(new Path (temp_path), true);
		}
		
		Job job = Job.getInstance(conf, "WordFreqStop");
		job.addCacheFile(new Path(args[0]+"stopwords/stop.txt").toUri());
		job.setJarByClass(WordFreqStop.class);
		
		job.setMapperClass(WordFreqStopMapper.class);
		job.setReducerClass(WordFreqStopReducer.class);	

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);	
		
		
		
		job.setNumReduceTasks(args.length > 2 ? Integer.parseInt(args[2]) : 1);	
		
		FileInputFormat.addInputPath(job, new Path(input_path));
		FileOutputFormat.setOutputPath(job, new Path(temp_path));

		if (fs.exists(new Path(output_path))) {
			fs.delete(new Path(output_path), true);
		}
		
		job.waitForCompletion(true);
		/*
		 * Sorting job
		 */
		
		Configuration sortConf = new Configuration();
		Job sortJob = Job.getInstance(sortConf, "SortWordFreqStop");
		sortJob.setJarByClass(WordFreqStop.class);
		
		sortJob.setMapperClass(WordFreqStopSortMapper.class);
		sortJob.setReducerClass(WordFreqStopSortReducer.class);	
		
		sortJob.setMapOutputKeyClass(IntegerWritable.class);
		sortJob.setMapOutputValueClass(Text.class);
		
		sortJob.setOutputKeyClass(Text.class);
		sortJob.setOutputValueClass(IntWritable.class);	
		
		sortJob.setNumReduceTasks(args.length > 2 ? Integer.parseInt(args[2]) : 1);
		
		FileInputFormat.addInputPath(sortJob, new Path(temp_path));
		FileOutputFormat.setOutputPath(sortJob,new Path(output_path));
		
		if (fs.exists(new Path(output_path))) {
			fs.delete(new Path(output_path), true);
		}
			
		sortJob.waitForCompletion(true);
		if (fs.exists(new Path (temp_path))){
			fs.delete(new Path (temp_path), true);
		}
	}
}

