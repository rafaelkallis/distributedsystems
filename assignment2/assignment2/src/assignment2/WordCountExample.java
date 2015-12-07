package assignment2;

import java.io.*;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class WordCountExample {

	public static class CountMapper extends Mapper<Object, Text, Text, IntWritable> {

		Set<String> stopWords;
		private final static IntWritable one = new IntWritable(1);
		//private Text word = new Text();
		
		@Override
		public void setup(Context context) throws IOException {
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			line = line.replaceAll(",", "");
			line = line.replaceAll("\\.", "");
			line = line.replaceAll("-", " ");
			line = line.replaceAll("\"", "");
			StringTokenizer tokenizer = new StringTokenizer(line);
			int movieId = Integer.parseInt(tokenizer.nextToken());
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken().toLowerCase();
				context.write(new Text("1"), one);
			}
		}
	}

	public static class CountReducer extends Reducer<Text, IntWritable, IntWritable, NullWritable> {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum++;
			}
			context.write(new IntWritable(sum), NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {

		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);
		
		job.setJarByClass(WordCountExample.class);

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