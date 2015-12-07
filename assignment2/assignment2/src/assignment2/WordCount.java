package assignment2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount {

	private static Text word = new Text("1");
	
	public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			
			context.write(WordCount.word, new IntWritable(tokenizer.countTokens()-1));
			
		}
	}
	
	public static class WordCountReducer extends Reducer<Text, IntWritable, IntWritable, NullWritable> {
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			IntWritable result = new IntWritable(0);
			for (IntWritable val : values) {
				result.set(result.get() + val.get());
			}		
			context.write(result, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {

		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "WordCount");

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setJarByClass(WordCount.class);

		job.setNumReduceTasks(args.length > 2 ? Integer.parseInt(args[2]) : 1);
		
		
		FileSystem fs = FileSystem.get(conf);
		// handle (e.g. delete) existing output path
		Path outputDestination = new Path(args[0]+args[1]);
		if (fs.exists(outputDestination)) {
			fs.delete(outputDestination, true);
		}

		// set output path & start job1
		FileInputFormat.addInputPath(job, new Path(args[0]+"data/plot_summaries.txt"));
		FileOutputFormat.setOutputPath(job, outputDestination);
		job.waitForCompletion(true);
	}
}
