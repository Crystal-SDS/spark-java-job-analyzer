package test.java;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import test.java.cases.flinkjava.FlinkJavaCountWordsTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    	FlinkJavaCountWordsTest.class,
})
public class FlinkJobAnalyzerTestSuite {}