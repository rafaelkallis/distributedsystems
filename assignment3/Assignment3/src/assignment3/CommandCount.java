package assignment3;

import java.util.List;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class CommandCount {
public static void main(String[] args) throws Exception {

		
		SparkConf conf = new SparkConf().setAppName("CommandCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(args[0], 1);
		
		
		JavaPairRDD<Integer,Integer> timestamps = lines.mapToPair(new PairFunction<String,Integer,Integer>() {
			@Override
			public Tuple2<Integer,Integer> call(String s) {
				s = s.toUpperCase().trim();
				if(s.startsWith("<COMMAND")){
					Integer timestamp = Integer.parseInt(s.replaceAll(".*TIMESTAMP=\"", "").replaceAll("\".*", ""));
					return new Tuple2<Integer,Integer>(timestamp/900000,1);
				}
				return new Tuple2<Integer,Integer>(0,0);
			}
		});
		
		JavaPairRDD<Integer,Integer> reduce = timestamps.reduceByKey(new Function2<Integer,Integer,Integer>(){
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0+arg1;
			}
		});

		JavaPairRDD<Integer,Integer> sorted = reduce.sortByKey();
		
		List<Tuple2<Integer,Integer>> output = sorted.take((int)reduce.count());
		
		File file = new File(args[1]);
		BufferedWriter outputBW = new BufferedWriter(new FileWriter(file));
		for(Tuple2<Integer,Integer> tuple : output){
			outputBW.write( tuple._1 + "\t"+ tuple._2 +"\n");
		}
		
		outputBW.close();
		sc.stop();
	}
}
