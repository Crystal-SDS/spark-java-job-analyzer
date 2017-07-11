package test.resources.test_jobs;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

public class SparkJavaSimpleSQL {
	
	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder().getOrCreate();
		
		/*Create schema for GridPocket CSV for efficiency
		 * date,index,sumHC,sumHP,type,vid,size,city,region,lat,lng */
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("date", DataTypes.DateType, true));
		fields.add(DataTypes.createStructField("index", DataTypes.FloatType, true));
		fields.add(DataTypes.createStructField("sumHC", DataTypes.FloatType, true));
		fields.add(DataTypes.createStructField("type", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("vid", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("size", DataTypes.ShortType, true));
		fields.add(DataTypes.createStructField("city", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("region", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("lat", DataTypes.LongType, true));
		fields.add(DataTypes.createStructField("lng", DataTypes.LongType, true));
		
		StructType schema = DataTypes.createStructType(fields);
		
		Dataset<Row> result = spark.read()
				.format("com.databricks.spark.csv")
				.option("header", "true")
				.schema(schema)
				.csv("swift2d://data1.lvm/meter-1MB.csv").cache();
				
		/*Get the aggregated consumption per meter of Paris meters in January 2015*/		
		result.select(col("vid"), col("index"), col("date"), col("city"))
			  .where(col("date").startsWith("2015-01-").and(col("city").equalTo("Paris")))
			  .groupBy(col("vid"))
			  .max("index")
			  .write().csv("/home/user/Desktop/q1.csv");
		
		/*Monthly consumption comparison of peak/off hours per energy type*/
		result.select(col("vid"), col("index"), col("date"), col("city"))
		  .where(col("date").leq("2015-01-01T00:00:00+01:00").and(col("city").equalTo("Rotterdam")))
		  .groupBy(col("vid"))
		  .max("index")
		  .write().csv("/home/user/Desktop/q2.csv");
		
		/*Get the top 3 most energy consuming cities the last year*/
		result.select(col("vid"), col("index"), col("date"), col("city"))
		  .where(col("date").leq("2015-01-01T00:00:00+01:00").and(col("city").equalTo("Rotterdam")))
		  .groupBy(col("vid"))
		  .max("index")
		  .write().csv("/home/user/Desktop/q3.csv");
	}

}
