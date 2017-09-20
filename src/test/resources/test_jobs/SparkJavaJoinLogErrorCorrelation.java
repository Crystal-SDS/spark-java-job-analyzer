package test.resources.test_jobs;

import java.time.Instant;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.mllib.stat.Statistics;

public class SparkJavaJoinLogErrorCorrelation {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkJavaJoinLogErrorCorrelation");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> logContainer1 = sc.textFile("swift2d://apache_logs.lvm/*");
		JavaRDD<Double> vectorContainer1 = logContainer1
					 .map(s -> {
							java.util.List<String> l = new java.util.ArrayList<String>(); 
							String[] a = s.split(",");
							for (String x: a) l.add(x); 
							return l;
						})
					 .filter(split -> (split.size()==10 && (split.get(8).startsWith("40") || split.get(8).startsWith("50"))))
					 .map(split -> split.get(3))
					 .map(s -> new Double(Instant.parse(s).toEpochMilli()));
		
		JavaRDD<String> logContainer2 = sc.textFile("swift2d://apache_logs2.lvm/*");
		JavaRDD<Double> vectorContainer2 = logContainer2
				.map(s -> {
					java.util.List<String> l = new java.util.ArrayList<String>(); 
					String[] a = s.split(",");
					for (String x: a) l.add(x); 
					return l;
				})
				.filter(split -> (split.size()==10 && (split.get(8).startsWith("40") || split.get(8).startsWith("50"))))
				.map(split -> split.get(3))
				.map(s -> new Double(Instant.parse(s).toEpochMilli()) - 604800000);
		
		Double correlation = Statistics.corr(vectorContainer1, vectorContainer2, "pearson");
		System.out.println("Correlation is: " + correlation);
			
	}
}
