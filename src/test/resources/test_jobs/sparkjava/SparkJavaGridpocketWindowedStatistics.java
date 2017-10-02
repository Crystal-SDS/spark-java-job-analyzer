package test.resources.test_jobs.sparkjava;
import java.time.Instant;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

public class SparkJavaGridpocketWindowedStatistics {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SimpleTextAnalysisSparkJava");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> distFile = sc.textFile("swift2d://gridpocket_140GB.lvm/*");
		List<Tuple2<String, Double>> sortedMeterSlotMaxValues = distFile.
				mapToPair(s -> {
					String[] split = s.split(",");
					String meterSlotKey = null;
					try {
						meterSlotKey = split[5] + "-" + String.valueOf(Instant.parse(
								split[0].substring(0, split[0].indexOf("+"))+"Z").toEpochMilli()/(24*3600*1000));
					} catch (Exception e) {	e.printStackTrace();}
					return new Tuple2<String, Double>(meterSlotKey, new Double(split[1]));
				})
				.reduceByKey((t1, t2) -> Math.max(t1, t2))
				.sortByKey()
				.collect();
				
		String previousMeter = null, currentMeter;
		for(int i=sortedMeterSlotMaxValues.size()-1; i>=0; i--) {
			currentMeter = sortedMeterSlotMaxValues.get(i)._1.substring(
					0, sortedMeterSlotMaxValues.get(i)._1.indexOf("-")); 
			if (previousMeter!=null && previousMeter.equals(currentMeter)){	
				sortedMeterSlotMaxValues.set(i+1, 
					new Tuple2<String, Double>(sortedMeterSlotMaxValues.get(i)._1,
						sortedMeterSlotMaxValues.get(i+1)._2-sortedMeterSlotMaxValues.get(i)._2));		
			}
			previousMeter = currentMeter;	
		}
		
		sc.parallelizePairs(sortedMeterSlotMaxValues)
					.mapToPair(t -> {
						Tuple4<Double, Double, Double, Long> values = new Tuple4<>(t._2, t._2, t._2, 1L);								
						return new Tuple2<String, Tuple4>(t._1.substring(t._1.indexOf("=")+1), values); 
					})
					.reduceByKey((t1, t2) -> {
						Double sumEnergyPerSlot = ((Double)t1._1()) + ((Double)t2._1());
						Double minEnergyPerSlot = Math.min((Double)t1._2(), (Double)t2._2());
						Double maxEnergyPerSlot = Math.max((Double)t1._3(), (Double)t2._3());
						Long count = ((Long)t1._4()) + ((Long)t2._4());
						return new Tuple4<Double, Double, Double, Long>(
								sumEnergyPerSlot, minEnergyPerSlot, maxEnergyPerSlot, count);
					})
					.sortByKey()
					.map(t -> {
						Tuple4<Double, Double, Double, Long> values = t._2;
						return new Tuple2<String, Tuple3<Double, Double, Double>>(t._1,
							new Tuple3<>((Double)values._1()/values._4(), (Double)values._2(), (Double)values._3()));
					})
					.saveAsTextFile("swift2d://output_pushdown.lvm/gridpocket_timeslot_results.csv");		
	}

}
