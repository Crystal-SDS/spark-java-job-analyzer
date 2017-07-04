package test.resources.test_jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class SparkJavaSimpleSQL {
	
	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder()			
				.getOrCreate();
		
		Dataset<Row> result = spark.read()
				.option("header", "true")
				.option("inferSchema", "true")
				.csv("swift2d://data1.lvm/meter-1MB.csv");
				
		/*Get aggregated the consumption per meter of Rotterdam meters in January 2015*/		
		result.select(col("vid"), col("index"), col("date"), col("city"))
			  .where(col("date").startsWith("2015-01-").and(col("city").equalTo("Rotterdam")))
			  .groupBy(col("vid"))
			  .max("index")
			  .write().csv("/home/user/Desktop/q1.csv");
		
				/*"SELECT SUBSTRING(date, 0, 10) as sDate, vid,"
				+ "min(sumHC) as minHC, max(sumHC) as maxHC, min(sumHP) as minHP, max(sumHP) as"
				+ " maxHP");	*/
	}

}
