package assignment3;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public final class AverageTime2 {
	
	public static final Double IGNORE_CODE = (double) -1;
	public static void main(String[] args) throws Exception {

		
		SparkConf conf = new SparkConf().setAppName("AverageTime").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(args[0], 1);
		
		
		JavaPairRDD<Integer,Double> timestamps = lines.mapToPair(new PairFunction<String,Integer,Double>() {
			@Override
			public Tuple2<Integer,Double> call(String s) {
				s = s.toUpperCase().trim();
				Double interval = IGNORE_CODE;
				if(s.startsWith("<COMMAND") && s.contains("TIMESTAMP2")){
					Double timestamp1 = Double.parseDouble(s.replaceAll(".*TIMESTAMP=\"", "").replaceAll("\".*", ""));
					Double timestamp2 = Double.parseDouble(s.replaceAll(".*TIMESTAMP2=\"", "").replaceAll("\".*", ""));
					interval = timestamp2-timestamp1;
				}
				return new Tuple2<Integer,Double>(1,interval);
			}
		});
		
		JavaPairRDD<Integer,Double> avg = timestamps.reduceByKey(new Function2<Double,Double,Double>(){
			@Override
			public Double call(Double arg0, Double arg1) throws Exception {
				return arg0==IGNORE_CODE ? arg1 : (arg1==IGNORE_CODE ? arg0 : ((arg0+arg1)/2));
			}
		});

		File file = new File(args[1]);
		BufferedWriter outputBW = new BufferedWriter(new FileWriter(file));
		Double avg_time = avg.take(1).get(0)._2;
		outputBW.write( avg_time +"\n");
		System.out.println(avg_time);
		outputBW.close();
		sc.stop();
	}
}
