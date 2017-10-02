package test.java;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import test.java.cases.javastreams.Java8CountWordsTest;
import test.java.cases.javastreams.Java8SimpleDistinctTest;
import test.java.cases.javastreams.Java8SimpleListCollectorTest;
import test.java.cases.javastreams.Java8SimpleLogAnalyzer2Test;
import test.java.cases.javastreams.Java8SimpleLogAnalyzer3Test;
import test.java.cases.javastreams.Java8SimpleLogAnalyzerTest;
import test.java.cases.javastreams.Java8SimpleMaxCollectorTest;
import test.java.cases.javastreams.Java8SimpleReduceTest;
import test.java.cases.javastreams.Java8SimpleReduceWithMathUseTest;
import test.java.cases.javastreams.Java8StreamVariableAssignmentsTest;
import test.java.cases.javastreams.Java8WordCountStreamsTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
		Java8SimpleDistinctTest.class,
    	Java8SimpleListCollectorTest.class,
        Java8SimpleLogAnalyzer2Test.class,
        Java8SimpleLogAnalyzer3Test.class,
        Java8SimpleLogAnalyzerTest.class,
        Java8SimpleMaxCollectorTest.class,
        Java8SimpleReduceTest.class,
        Java8SimpleReduceWithMathUseTest.class,
        Java8StreamVariableAssignmentsTest.class,
        Java8WordCountStreamsTest.class,
        Java8CountWordsTest.class
})
public class JavaStreamsJobAnalyzerTestSuite {}