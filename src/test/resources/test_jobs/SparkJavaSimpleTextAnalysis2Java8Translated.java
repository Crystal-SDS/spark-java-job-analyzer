package test.resources.test_jobs; import org.apache.spark.SparkConf; import java.util.stream.Stream; import org.apache.spark.api.java.JavaSparkContext; public class SparkJavaSimpleTextAnalysis2Java8Translated { public static void main(String[] args) { SparkConf conf = new SparkConf().setAppName("SimpleTextAnalysisSparkJava"); JavaSparkContext sc = new JavaSparkContext(conf); Stream<String> distFile = sc.textFile("swift2d://data1.lvm/hamlet.txt"); distFile.map(s -> s.split(" ").length).reduce((a, b) -> a + b); } }
