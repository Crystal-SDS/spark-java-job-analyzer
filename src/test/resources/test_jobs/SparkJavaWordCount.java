package test.resources.test_jobs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class SparkJavaWordCount {
	
	public static void main(String[] args) {
		
		long timeInMillis = System.currentTimeMillis();
		
		SparkConf conf = new SparkConf().setAppName("SparkJavaWordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> textFile = sc.textFile("swift2d://data1.lvm/hamlet.txt");
		JavaPairRDD<String, Integer> counts = textFile
		    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
		    .map(word -> word.replaceAll("[^a-zA-Z]", "").toLowerCase().trim())
		    .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
		    .reduceByKey((a, b) -> a + b);
		
		counts.saveAsTextFile("swift2d://data1.lvm/hamlet_result.txt");		
		
		try {
			Files.write(Paths.get("./wordcount_result" + timeInMillis + ".dat"), 
					String.valueOf(System.currentTimeMillis() - timeInMillis).getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}