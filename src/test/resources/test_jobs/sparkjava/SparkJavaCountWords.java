package test.resources.test_jobs.sparkjava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkJavaCountWords {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkJavaCountWords");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> distFile = sc.textFile("swift2d://data1.lvm/hamlet.txt");
		distFile.map(s -> new Long(s.split(" ").length)).reduce((a, b) -> (Long) a + b);		
	}

}
