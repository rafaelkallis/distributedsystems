package assignment3;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class CommandFrequency {
public static void main(String[] args) throws Exception {

		
		SparkConf conf = new SparkConf().setAppName("CommandFrequency").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(args[0], 1);
		
		
		JavaPairRDD<String,Integer> timestamps = lines.mapToPair(new PairFunction<String,String,Integer>() {
			@Override
			public Tuple2<String,Integer> call(String s) {
				String key = "IGNORE_ME";
				if(s.trim().startsWith("<Command")){
					String type = s.replaceAll(".* _type=\"", "").replaceAll("\".*", "");
					if(type.equals("EclipseCommand")){
						String eclipseType = s.replaceAll(".*commandID=\"", "").replaceAll("\".*", "");
						key = type+":"+eclipseType;
					}else{
						key = type;
					}
					
				}
				return new Tuple2<String,Integer>(key,1);
			}
		});
		
		JavaPairRDD<String,Integer> reduce = timestamps.reduceByKey(new Function2<Integer,Integer,Integer>(){
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0+arg1;
			}
		});
		
		JavaPairRDD<Integer,String> swapped = reduce.mapToPair(new PairFunction<Tuple2<String,Integer>,Integer,String>(){

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> arg0) throws Exception {
				return arg0.swap();
			}
			
		});
		
		JavaPairRDD<String,Integer> swappedBack = swapped.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer,String>,String,Integer>(){

			@Override
			public Tuple2<String, Integer> call(Tuple2<Integer, String> arg0) throws Exception {
				return arg0.swap();
			}
			
		});
		
		List<Tuple2<String,Integer>> output = swappedBack.take((int)reduce.count());
		
		File file = new File(args[1]);
		BufferedWriter outputBW = new BufferedWriter(new FileWriter(file));
		for(Tuple2<String,Integer> tuple : output){
			if(!tuple._1.equals("IGNORE_ME")){
				outputBW.write( tuple._1 + "\t"+ tuple._2 +"\n");
			}
		}
		
		outputBW.close();
		sc.stop();
	}
}
