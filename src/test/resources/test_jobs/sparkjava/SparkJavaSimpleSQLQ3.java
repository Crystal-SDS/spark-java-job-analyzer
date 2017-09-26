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

public class SparkJavaSimpleSQLQ3 {
	
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
				.csv("swift2d://data1.lvm/meter-new-15MB.csv").cache(); //.csv("swift2d://gridpocket_140GB.lvm/*");

		/*Get the an ordered list of cities by total energy consumption in the last 3 years*/		
		result.select(col("vid"), col("index"), col("date"), col("city"))
			  .where(col("date").between("2014-01-01T00:00:00+01:00", "2016-12-31T11:59:59+01:00"))
			  .groupBy(col("city"), col("vid"))
			  .max("index")
			  .dropDuplicates()
			  .drop(col("vid"))
			  .groupBy(col("city"))
			  .sum("max(index)")
			  .sort("sum(max(index))")
			  .repartition(1)
			  .write().csv("/home/user/Desktop/q3.csv");		
	}

}
