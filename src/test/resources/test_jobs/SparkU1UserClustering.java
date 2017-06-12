package test.resources.test_jobs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;

import scala.Tuple2;

public class SparkU1UserClustering {
	
public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkU1UserClustering");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> distFile = sc.textFile("swift2d://data1.lvm/1_u1.csv");

		//Here we have items in tuplesPair as (user, (op_name, count))
		JavaPairRDD<String, Tuple2<String, Integer>> userOpsTuples = distFile
			.filter(s -> s.startsWith("storage_done"))
			.map(s -> s.split(","))
			.filter(s -> s[19].equals("PutContentResponse") || s[19].equals("GetContentResponse") ||
					s[19].equals("MakeResponse") || s[19].equals("Unlink") || s[19].equals("MoveResponse"))
			.mapToPair(s -> new Tuple2<String, Integer>(s[33] + "-" + s[19], 1))
			.reduceByKey((a, b) -> a + b)
			.mapToPair(t -> new Tuple2<String, Tuple2<String, Integer>>(t._1.split("-")[0], 
					new Tuple2<String, Integer>(t._1.split("-")[1], t._2)));
		
		//Next, we have to convert these tuples into (user, [count_op1, count_op2,...]) for the clustering algorithm
		userOpsTuples.groupByKey().map(t -> {
			double[] values = new double[5];
	    	for (Tuple2<String, Integer> theTuple: t._2) {
	    		switch (theTuple._1) {
					case "PutContentResponse":
						values[0] = theTuple._2;
						break;
					case "GetContentResponse":
						values[1] = theTuple._2;
						break;
					case "MakeResponse":
						values[2] = theTuple._2;
						break;
					case "Unlink":
						values[3] = theTuple._2;
						break;
					case "MoveResponse":
						values[4] = theTuple._2;
						break;
					default:
						break;
				}
	    	}
		    return new Tuple2<String, Vector>(t._1, Vectors.dense(values));
		}).saveAsTextFile("/home/user/Desktop/result.csv");	
	}
}
