package test.resources.test_jobs.sparkjava;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import scala.collection.Iterator;

public class SparkJavaDNASimilarity {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SimpleTextAnalysisSparkJava");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> distFile = sc.textFile("swift2d://dna_data.lvm/*");
		JavaRDD<Vector> gattacaDNAToVectors = distFile
				.filter(s -> s.startsWith("A") || s.startsWith("C") || 
							 s.startsWith("G") || s.startsWith("T"))
				.filter(s -> s.contains("GATTACA"))
				.map(s -> {
					ArrayList<Double> result = new ArrayList<>();
					for (char c: s.toCharArray())
						result.add(new Double(((int)c)-64));
					return result;
				})
				.map(a -> Vectors.dense(a.stream().mapToDouble(Double::doubleValue).toArray()))
				.cache();
		
		RowMatrix rowMatrix = new RowMatrix(gattacaDNAToVectors.rdd());
		//Execute pair-to-pair sequence similarity algorithm
		Iterator<MatrixEntry> similarityIterator = rowMatrix.columnSimilarities().entries().toLocalIterator();
		while (similarityIterator.hasNext())
			System.err.println(similarityIterator.next());
		
	}

}
