package test.resources.test_jobs;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class SparkJavaSimpleSQLQ2 {
	
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
