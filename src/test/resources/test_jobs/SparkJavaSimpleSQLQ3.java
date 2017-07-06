package test.resources.test_jobs;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class SparkJavaSimpleSQLQ3 {
	
	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder().getOrCreate();
		
		Dataset<Row> result = spark.read()
				.option("header", "true")
				.option("inferSchema", "true")
				.csv("swift2d://gridpocket_140GB.lvm/*");
		
		/*Get the an ordered list of cities by total energy consumption in the last 3 years*/
		result.select(col("vid"), col("index"), col("date"), col("city"))
			  .where(col("date").between("2014-01-01T00:00:00+01:00", "2016-12-31T11:59:59+01:00"))
			  .groupBy(col("city"), col("vid"))
			  .max("index")
			  .dropDuplicates()
			  .drop(col("vid"))
			  .toJavaRDD()
			  .mapToPair(r -> new Tuple2<>(r.getString(0), r.getDouble(1)))
			  .reduceByKey((a,b) -> a+b)
			  .repartition(1)
			  .mapToPair(x -> x.swap())
			  .sortByKey(false)
			  .mapToPair(x -> x.swap())
			  .saveAsTextFile("/home/lab144/raul/q3.csv");
	}

}
