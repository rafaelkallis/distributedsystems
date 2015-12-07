package assignment2;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.Text;

public class Index {
	
	public static class IndexMapper extends Mapper<LongWritable,Text,Text,IDTuple>{
		@Override
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString();
			line = line.replaceAll(",", " ");
			line = line.replaceAll("-", " ");
			line = line.replaceAll("\\.", " ");
			line = line.toLowerCase();

			StringTokenizer tokenizer = new StringTokenizer(line);
			String movie_id = tokenizer.nextToken();
			while(tokenizer.hasMoreTokens()){
				String term = tokenizer.nextToken();
				IDTuple tuple = new IDTuple(movie_id);
				context.write(new Text(term),tuple);
			}
		}
	}
	
	public static class IndexReducer extends Reducer<Text,IDTuple,Text,Text>{
		@Override
		public void reduce(Text key, Iterable<IDTuple> values,Context context) throws IOException, InterruptedException{
			String line = "";
			HashMap<String, Integer> inverted_map = new HashMap<String, Integer>();
			for (IDTuple tuple : values) {
				if (inverted_map.containsKey(tuple.id)) {
					inverted_map.put(tuple.id, inverted_map.get(tuple.id) + 1);
				}else{
					inverted_map.put(tuple.id, new Integer(1));
				}
			}
			Object[] movie_ids = inverted_map.keySet().toArray();
			Object[] counts =  inverted_map.values().toArray();
			for (int i = 0; i < counts.length; i++) {
				line += movie_ids[i] + "," + counts[i] + " ";
			}
			if (line.length() > 0) line.substring(0, line.length() - 1);
			context.write(key, new Text(line));
		}
	}
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		
		final String input_path		= args[0]+"data/plot_summaries.txt";

		final String output_path	= args[0]+args[1];
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Job job = Job.getInstance(conf, "Index");
		job.setJarByClass(Index.class);
		
		job.setMapperClass(IndexMapper.class);
		job.setReducerClass(IndexReducer.class);	

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IDTuple.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);	
		
		job.setNumReduceTasks(args.length > 2 ? Integer.parseInt(args[2]) : 1);	
		
		FileInputFormat.addInputPath(job, new Path(input_path));
		FileOutputFormat.setOutputPath(job, new Path(output_path));

		if (fs.exists(new Path(output_path))) {
			fs.delete(new Path(output_path), true);
		}
		
		job.waitForCompletion(true);

	}
}
