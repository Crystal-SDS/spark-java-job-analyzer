package test.resources.test_jobs.sparkjava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class SparkJavaGridpocketExploratoryQuery {
	
	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder().getOrCreate();
		
		/*Create schema for GridPocket CSV for efficiency
		 * date,index,sumHC,sumHP,type,vid,size,city,region,lat,lng */
		
		SparkConf conf = new SparkConf().setAppName("SparkJavaGridpocketExploratoryQuery");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> textFile = sc.textFile("swift2d://data1.lvm/hamlet.txt");
		JavaPairRDD<String, Double> meterEnergyPairs = textFile
				.filter(s -> !s.startsWith("date") && s.startsWith("2012"))
				.map(s -> {
					java.util.List<String> l = new java.util.ArrayList<String>(); 
					String[] a = s.split(",");
					for (String x: a) l.add(x); 
					return l;
				})
				.filter(a -> a.get(7).equals("Paris"))
			    .mapToPair(a -> new Tuple2<String, Double>(a.get(5), Double.valueOf(a.get(1))))
			    .reduceByKey((a, b) -> Math.max(a, b));
		
		meterEnergyPairs.saveAsTextFile("swift2d://data1.lvm/hamlet_result.txt");
	}

}
