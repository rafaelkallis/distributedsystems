package assignment2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Join {

	public static class JoinMovieMapper extends Mapper<LongWritable,Text,IntWritable, Text>{
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] row = value.toString().split("\\t", -1);
			IntWritable movie_id = new IntWritable(Integer.parseInt(row[0]));
			Text movie = new Text(("M"+row[2]));
			context.write(movie_id, movie);
		}
	}
	public static class JoinActorMapper extends Mapper<LongWritable,Text,IntWritable, Text>{
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] row = value.toString().split("\\t", -1);
			IntWritable movie_id = new IntWritable(Integer.parseInt(row[0]));
			Text actor = new Text(("A"+row[8]));
			context.write(movie_id, actor);
		}
	}
	public static class JoinReducer extends Reducer<IntWritable,Text,IntWritable,Text>{
		@Override
		public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException{
			String line = "";
			for(Text text : values){
				if(text.toString().startsWith("M")){
					line = text.toString().substring(1) + line;
				}else if(text.toString().startsWith("A")){
					line = line + "," + text.toString().substring(1);
				}
			}

			System.out.println(line);
			context.write(key, new Text(line));
		}
	}
	public static void main(String args[]) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		
		final String input_path1		= args[0]+"data/movie.metadata.tsv";
		final String input_path2		= args[0]+"data/character.metadata.tsv";
		final String output_path		= args[0]+args[1];
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Job job = Job.getInstance(conf, "Join");
		job.setJarByClass(Join.class);
		
		job.setReducerClass(JoinReducer.class);	

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);	
		
		job.setNumReduceTasks(args.length > 2 ? Integer.parseInt(args[2]) : 1);	
		
		MultipleInputs.addInputPath(job, new Path(input_path1), TextInputFormat.class, JoinMovieMapper.class);
		MultipleInputs.addInputPath(job, new Path(input_path2), TextInputFormat.class, JoinActorMapper.class);       
		TextOutputFormat.setOutputPath(job, new Path(output_path));

		if (fs.exists(new Path(output_path))) {
			fs.delete(new Path(output_path), true);
		}
		
		job.waitForCompletion(true);
	}
	
}
