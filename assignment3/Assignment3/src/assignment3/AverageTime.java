package assignment3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.DecimalFormat;

public final class AverageTime {
	
	public static Integer last_timestamp = null;
	public static void main(String[] args) throws Exception {

		
		SparkConf conf = new SparkConf().setAppName("AverageTime").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile(args[0], 1);
		
		
		JavaRDD<Integer> timestamps = lines.map(new Function<String,Integer>() {
			@Override
			public Integer call(String s) {
				s = s.toUpperCase().trim();
				if(s.contains(" TIMESTAMP=")){
					last_timestamp = Integer.parseInt(s.replaceAll(".*TIMESTAMP=\"", "").replaceAll("\".*", ""));
				}
				if(s.startsWith("<COMMAND")){
					return 1;
				}else{					
					return 0;
				}
			}
		});
		
		int n_commands = timestamps.reduce(new Function2<Integer,Integer,Integer>(){
			@Override
			public Integer call(Integer arg0,Integer arg1) throws Exception {
				Integer toReturn = arg0+arg1;
				return toReturn;
			}	
		});

		File file = new File(args[1]);
		BufferedWriter outputBW = new BufferedWriter(new FileWriter(file));
		Double avg_time =  ((double)last_timestamp / n_commands);
		outputBW.write( new DecimalFormat("#.##").format(avg_time));
		outputBW.close();
		sc.stop();
	}
}
