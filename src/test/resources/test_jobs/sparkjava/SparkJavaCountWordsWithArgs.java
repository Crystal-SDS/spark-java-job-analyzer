package test.resources.test_jobs.sparkjava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkJavaCountWordsWithArgs {
	
	/**
	 * args[0] is the file path, for example: "swift2d://data1.lvm/hamlet.txt"
	 * args[1] is the name of the app, for example: "SparkJavaCountWordsWithArgs"
	 * @param args
	 */
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName(args[1]);
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> distFile = sc.textFile(args[0]);
		Long result = distFile.map(s -> new Long(s.split(" ").length)).reduce((a, b) -> (Long) a + b);
		System.err.println(result);

	}
	
}
