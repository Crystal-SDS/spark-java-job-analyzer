package test.java.cases.javastreams;

import test.java.AbstractAnalyzerTest;
import test.resources.test_jobs.javastreams.Java8SimpleLogAnalyzer2;

public class Java8SimpleLogAnalyzer2Test extends AbstractAnalyzerTest{	
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		this.inputStorletFile = "test_data/hamlet.txt";
		this.analyticsJob = new Java8SimpleLogAnalyzer2();
		this.jobToAnalyze = "/test_jobs/javastreams/Java8SimpleLogAnalyzer2.java";
	}	
}