package test.resources.test_jobs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkU1UserClustering {
	
public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkU1UserClustering");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> distFile = sc.textFile("swift2d://data1.lvm/1_u1.csv");
		
		JavaRDD<Tuple2<String, Tuple2<String, Integer>>> tuplesPair = distFile
			.filter(s -> s.startsWith("storage_done"))
			.map(s -> s.split(","))
			.filter(s -> s[19].equals("PutContentResponse") || s[19].equals("GetContentResponse") ||
					s[19].equals("MakeResponse") || s[19].equals("Unlink") || s[19].equals("MoveResponse"))
			.mapToPair(s -> new Tuple2<String, Integer>(s[33] + "-" + s[19], 1))
			.reduceByKey((a, b) -> a + b)
			.map(t -> new Tuple2<String, Tuple2<String, Integer>>(t._1.split("-")[0], 
					new Tuple2<String, Integer>(t._1.split("-")[1], t._2)));
		
		tuplesPair.saveAsTextFile("/home/user/Desktop/result.csv");	
	}
}
