package test.resources.test_jobs;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class SparkJavaSimpleSQLQ2 {
	
	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder().getOrCreate();
		
		Dataset<Row> result = spark.read()
				.option("header", "true")
				.option("inferSchema", "true")
				.csv("swift2d://gridpocket_140GB.lvm/*"); //.csv("swift2d://data1.lvm/meter-new-15MB.csv");//
		
		/*Monthly consumption comparison of peak/off hours per energy type for all times*/
		//Dataset<Row> consumptionsHC = 
		result.select(col("vid"), col("sumHC"), col("date"), col("type"))
				.groupBy(col("date").substr(0, 7), col("type"), col("vid"))
				.agg(max(col("sumHC")))
				.dropDuplicates()
				.drop(col("vid"))
				.toJavaRDD()
				.mapToPair(r -> new Tuple2<>(r.getString(0)+"-"+r.getString(1), r.getDouble(2)))
				.reduceByKey((a,b) -> a+b)
				.repartition(1)
				.sortByKey()
				.saveAsTextFile("/home/user/Desktop/q2/sumHC.txt"); 
				//.saveAsTextFile("/home/user/Desktop/q2_pushdown/sumHC.txt");
		
		result.select(col("vid"), col("sumHP"), col("date"), col("type"))
				.groupBy(col("date").substr(0, 7), col("type"), col("vid"))
				.agg(max(col("sumHP")))
				.dropDuplicates()
				.drop(col("vid"))
				.toJavaRDD()
				.mapToPair(r -> new Tuple2<>(r.getString(0)+"-"+r.getString(1), r.getDouble(2)))
				.reduceByKey((a,b) -> a+b)
				.repartition(1)
				.sortByKey()
				.saveAsTextFile("/home/user/Desktop/q2/sumHP.txt"); 
				//.saveAsTextFile("/home/user/Desktop/q2/sumHP.txt");
	}

}
