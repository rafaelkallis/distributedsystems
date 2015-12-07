package assignment2;

import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordFreq {	
	
	
	/*
	 * WordFreq Job
	 */
	public static class WordFreqMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
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
				context.write(new Text(tokenizer.nextToken()), new IntWritable(1));
			}
		}
	}
	
	public static class WordFreqReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
				
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
	public static class WordFreqSortMapper extends Mapper<LongWritable, Text, IntegerWritable , Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String[] line = value.toString().split("\t");
			Text word = new Text(line[0]);

			IntegerWritable hits = new IntegerWritable(Integer.parseInt(line[1]));
			context.write(hits, word);
		}
	}
	
	public static class WordFreqSortReducer extends Reducer<IntegerWritable, Text , Text , IntegerWritable>{
			
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
		 * WordFreq Job
		 */
		
		if (fs.exists(new Path (temp_path))){
			fs.delete(new Path (temp_path), true);
		}
		
		Job job = Job.getInstance(conf, "WordFreq");
		job.setJarByClass(WordFreq.class);
		
		job.setMapperClass(WordFreqMapper.class);
		job.setReducerClass(WordFreqReducer.class);	

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
		Job sortJob = Job.getInstance(sortConf, "SortWordFreq");
		sortJob.setJarByClass(WordFreq.class);
		
		sortJob.setMapperClass(WordFreqSortMapper.class);
		sortJob.setReducerClass(WordFreqSortReducer.class);	
		
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
