package test.resources.test_jobs.sparkjava;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;
import static org.apache.spark.sql.functions.*;

public class SparkJavaSimpleSQLQ4 {
	
	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder().getOrCreate();
		
		/*Create schema for GridPocket CSV for efficiency
		 * date,index,sumHC,sumHP,type,vid,size,city,region,lat,lng */
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("date", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("index", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("sumHC", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("sumHP", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("type", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("vid", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("size", DataTypes.ShortType, true));
		fields.add(DataTypes.createStructField("city", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("region", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("lat", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("lng", DataTypes.DoubleType, true));
		
		StructType schema = DataTypes.createStructType(fields);
		
		Dataset<Row> result = spark.read()
				.format("com.databricks.spark.csv")
				.option("header", "true")
				.schema(schema)
				.csv("swift2d://data1.lvm/meter-new-15MB.csv").cache(); 
		
		/*Get the geographic coordinates of the energy meter that reported most energy consumption of type electricity in 2012*/		
		result.select(col("vid"), col("index"), col("type"), col("lat"), col("lng"))
			  .where(col("date").startsWith("2012").and(col("type").equalTo("elec")))
			  .groupBy(col("vid"), col("lat"), col("lng"))
			  .max("index")
			  .dropDuplicates()
			  .repartition(1)
			  .limit(1)
			  .write().csv("/home/user/Desktop/q4.csv");	
	}

}
