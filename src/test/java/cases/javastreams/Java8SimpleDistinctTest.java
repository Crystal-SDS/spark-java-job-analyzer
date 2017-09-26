package test.java.cases.javastreams;

import test.java.AbstractAnalyzerTest;
import test.resources.test_jobs.javastreams.Java8SimpleDistinct;

public class Java8SimpleDistinctTest extends AbstractAnalyzerTest{	
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		this.inputStorletFile = "test_data/hamlet.txt";
		this.analyticsJob = new Java8SimpleDistinct();
		this.jobToAnalyze = "/test_jobs/javastreams/Java8SimpleDistinct.java";
	}	
}