package test.java;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import test.java.cases.sparkjava.SparkJavaCountWordsTest;
import test.java.cases.sparkjava.SparkJavaJoinLogErrorCorrelationTest;
import test.java.cases.sparkjava.SparkPageRankTest;
import test.java.cases.sparkjava.SparkSimpleTextAnalysisTest;
import test.java.cases.sparkjava.SparkU1UserClusteringTest;
import test.java.cases.sparkjava.SparkWordCountTest;
import test.java.cases.sparkjava.SparkWordCountTest2;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    	SparkSimpleTextAnalysisTest.class,
    	SparkJavaCountWordsTest.class,
    	SparkWordCountTest.class,
    	SparkWordCountTest2.class,
    	SparkPageRankTest.class,
    	SparkU1UserClusteringTest.class,
    	SparkJavaJoinLogErrorCorrelationTest.class
})
public class SparkJobAnalyzerTestSuite {}