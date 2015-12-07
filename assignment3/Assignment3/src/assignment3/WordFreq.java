/*
 * (c) University of Zurich 2015
 */

package assignment3;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class WordFreq {
	public static void main(String[] args) throws Exception {

		
		SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(args[0], 1);

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s) {
				//String str = s.replace("-", " ").replace("#", " ").toUpperCase();
				//return Arrays.asList(str.split(" "));
				return Arrays.asList(s.split(" "));
			}
		});

		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		JavaPairRDD<Integer, String> swappedPair = counts
				.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					@Override
					public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
						return item.swap();
					}

				});
		JavaPairRDD<Integer, String> swappedPairSorted = swappedPair.sortByKey(false);
		JavaPairRDD<String, Integer> swappedPairSortedSwapBack = swappedPairSorted
				.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(Tuple2<Integer, String> item) throws Exception {
						return item.swap();
					}

				});

		List<Tuple2<String, Integer>> output = swappedPairSortedSwapBack.take(10);

		File file = new File(args[1]);
		BufferedWriter outputBW = new BufferedWriter(new FileWriter(file));

		for (Tuple2<?, ?> tuple : output) {
			outputBW.write(tuple._1() + "\t" + tuple._2() +"\n");
		}
		outputBW.close();
		sc.stop();
	}
}
