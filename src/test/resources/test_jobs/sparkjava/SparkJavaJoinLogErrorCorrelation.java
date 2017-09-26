package test.resources.test_jobs.sparkjava;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.mllib.stat.Statistics;

import scala.Tuple2;

public class SparkJavaJoinLogErrorCorrelation {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkJavaJoinLogErrorCorrelation");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		long hourTimeSlots = 650;
		
		JavaRDD<String> logContainer1 = sc.textFile("swift2d://apache_logs.lvm/*");
		JavaPairRDD<Integer, Integer> errorsPerHourC1 = logContainer1
					 .map(s -> {
							java.util.List<String> l = new java.util.ArrayList<String>(); 
							String[] a = s.split(" ");
							for (String x: a) l.add(x); 
							return l;
						})
					 .filter(split -> (split.size()== 9 && (split.get(8).startsWith("40") || split.get(8).startsWith("50"))))
					 .map(split -> split.get(3).substring(1, split.get(3).length()))
					 .map(s -> {
						 try{ 
							 return (int) (new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss").parse(s).getTime()/3600000 - 223321L);
						 }catch (Exception e) {e.printStackTrace();} return null;})					
					 .mapToPair(l -> new Tuple2<Integer, Integer>(l, 1))
					 .reduceByKey((a, b) -> a + b);
			
		JavaRDD<String> logContainer2 = sc.textFile("swift2d://apache_logs2.lvm/*");
		JavaPairRDD<Integer, Integer> errorsPerHourC2 = logContainer2
					.map(s -> {
						java.util.List<String> l = new java.util.ArrayList<String>(); 
						String[] a = s.split(" ");
						for (String x: a) l.add(x); 
						return l;
					})
					.filter(split -> (split.size()==9 && (split.get(8).startsWith("40") || split.get(8).startsWith("50"))))
					.map(split -> split.get(3).substring(1, split.get(3).length()))
					.map(s -> {
						 try{ 
							 return (int) (new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss").parse(s).getTime()/3600000 - 223321L);
						 }catch (Exception e) {e.printStackTrace();} return null;})					
					 .mapToPair(l -> new Tuple2<Integer, Integer>(l, 1))
					 .reduceByKey((a, b) -> a + b);
		
		List<Double> vectorContainer1 = new ArrayList<>();
		List<Double> vectorContainer2 = new ArrayList<>();
		for (long i=0; i<hourTimeSlots; i++){
			vectorContainer1.add(0.0);
			vectorContainer2.add(0.0);
		}
		Iterator<Tuple2<Integer, Integer>> errorsPerHourC1Iterator = errorsPerHourC1.toLocalIterator();
		while (errorsPerHourC1Iterator.hasNext()){
			Tuple2<Integer, Integer> tuple = errorsPerHourC1Iterator.next();
			vectorContainer1.set(tuple._1(), Double.valueOf(tuple._2()));
		}			
		Iterator<Tuple2<Integer, Integer>> errorsPerHourC2Iterator = errorsPerHourC2.toLocalIterator();
		while (errorsPerHourC2Iterator.hasNext()){
			Tuple2<Integer, Integer> tuple = errorsPerHourC2Iterator.next();
			vectorContainer2.set((tuple._1()), new Double(tuple._2()));
		}	
				
		Double correlation = Statistics.corr(sc.parallelize(vectorContainer1), sc.parallelize(vectorContainer2), "pearson");
		System.out.println("Correlation is: " + correlation);
		System.err.println("Correlation is: " + correlation);
			
	}
}
