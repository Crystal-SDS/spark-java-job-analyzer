package test.resources.test_jobs;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import static org.apache.spark.sql.functions.*;

public class SparkJavaSimpleSQLQ1 {
	
	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder().getOrCreate();
		
		Dataset<Row> result = spark.read()
				.option("header", "true")
				.option("inferSchema", "true")
				.csv("swift2d://data1.lvm/meter-new-15MB.csv");//.csv("swift2d://gridpocket_140GB.lvm/*");
		
		/*Get the aggregated consumption per meter of Paris meters in January 2015*/	
		result.select(col("vid"), col("index"), col("date"), col("city"))
		  .where(col("date").between("2015-01-01T00:00:00+01:00", "2015-01-31T23:59:59+01:00")
				 .and(col("city").equalTo("Paris")))
		  .groupBy(col("vid"))
		  .agg(max(col("index")), min(col("index")))	
		  .dropDuplicates()
		  .toJavaRDD()
		  .map(r -> new Tuple2<>(r.getString(0), r.getDouble(1)-r.getDouble(2)))
		  .repartition(1)
		  .saveAsTextFile("/home/user/Desktop/q1.csv");//.saveAsTextFile("/home/lab144/raul/q1.txt");
	}

}
