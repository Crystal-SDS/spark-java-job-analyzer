package test.resources.test_jobs.sparkjava;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

public class SparkJavaGridpocketWindowedStatisticsWithArgs {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkJavaGridpocketWindowedStatistics");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> distFile = sc.textFile(args[0]);
		
		distFile.filter(s -> !s.startsWith("date"))
				.mapToPair(s -> {
					int minutes = Integer.parseInt(args[1]);
					String[] split = s.split(",");
					String meterSlotKey = null;
					try {
						meterSlotKey = split[5] + "-" + String.valueOf(Instant.parse(
								split[0].substring(0, split[0].indexOf("+"))+"Z").toEpochMilli()/(minutes*60*1000));
					} catch (Exception e) {	e.printStackTrace();}
					return new Tuple2<String, Double>(meterSlotKey, new Double(split[1]));
				})
				.reduceByKey((t1, t2) -> Math.max(t1, t2))
				.mapToPair(t -> new Tuple2<String, Tuple2<String, Double>>(t._1.substring(0, t._1.indexOf("-")),
							new Tuple2<String, Double>(t._1.substring(t._1.indexOf("-")+1), t._2)))
				.groupByKey()
				.mapPartitionsToPair(meterPartitions -> {
					List<Tuple2<String, Iterable<Tuple2<String, Double>>>> finalResults = new ArrayList<>();
					while (meterPartitions.hasNext()) {
						Tuple2<String, Iterable<Tuple2<String, Double>>> meterTuple = meterPartitions.next();
						List<Tuple2<String, Double>> toSortSlots = new ArrayList<>();
						for (Tuple2<String, Double> slotTuple: meterTuple._2())
							toSortSlots.add(slotTuple);
						Collections.sort(toSortSlots,  new TupleComparator2());
						
						List<Tuple2<String, Double>> perSlotMeterEnergy = new ArrayList<>();
						Double previousMeter = null;
						for (Tuple2<String, Double> slotTuple: toSortSlots) {	
							if (previousMeter!=null){	
								perSlotMeterEnergy.add(new Tuple2<String, Double>(slotTuple._1(), slotTuple._2()-previousMeter));		
							} else perSlotMeterEnergy.add(slotTuple);
							previousMeter =  slotTuple._2();	
						}
						finalResults.add(new Tuple2<String, Iterable<Tuple2<String,Double>>>(meterTuple._1, perSlotMeterEnergy));
					}
					return finalResults.iterator();
					}, true)
				.flatMapToPair(meterPartitions -> meterPartitions._2.iterator())
				.mapToPair(t -> {
					Tuple4<Double, Double, Double, Long> values = new Tuple4<>(t._2, t._2, t._2, 1L);								
					return new Tuple2<String, Tuple4>(t._1, values); 
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
					int minutes = Integer.parseInt(args[1]);
					Tuple4<Double, Double, Double, Long> values = t._2;
					String date = Instant.ofEpochMilli(Long.valueOf(t._1)*minutes*60*1000).toString();
					return new Tuple2<String, Tuple3<Double, Double, Double>>(date,
						new Tuple3<>((Double)values._1()/values._4(), (Double)values._2(), (Double)values._3()));
				})
				.coalesce(1)
				.saveAsTextFile("swift2d://output_pushdown.lvm/gridpocket_timeslot_results.csv");
	}

}

class TupleComparator2 implements Comparator<Tuple2<String, Double>>, Serializable {
	private static final long serialVersionUID = 1L;

	public int compare(Tuple2<String, Double> tupleA, Tuple2<String, Double> tupleB) {
    	return tupleA._1.compareTo(tupleB._1);
	}
}

