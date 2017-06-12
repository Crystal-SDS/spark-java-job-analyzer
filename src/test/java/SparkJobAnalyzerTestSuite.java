package test.java;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import test.java.cases.SparkPageRankTest;
import test.java.cases.SparkSimpleTextAnalysisTest;
import test.java.cases.SparkSimpleTextAnalysisTest2;
import test.java.cases.SparkU1UserClusteringTest;
import test.java.cases.SparkWordCountTest;
import test.java.cases.SparkWordCountTest2;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    	SparkSimpleTextAnalysisTest.class,
    	SparkSimpleTextAnalysisTest2.class,
    	SparkWordCountTest.class,
    	SparkWordCountTest2.class,
    	SparkPageRankTest.class,
    	SparkU1UserClusteringTest.class
})
public class SparkJobAnalyzerTestSuite {}