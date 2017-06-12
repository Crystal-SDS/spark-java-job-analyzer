package test.java.cases;

import test.java.AbstractAnalyzerTest;
import test.resources.test_jobs.Java8SimpleLogAnalyzer4;

public class Java8SimpleLogAnalyzer4Test extends AbstractAnalyzerTest{	
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		this.inputStorletFile = "test_data/hamlet.txt";
		this.analyticsJob = new Java8SimpleLogAnalyzer4();
		this.jobToAnalyze = "/test_jobs/Java8SimpleLogAnalyzer4.java";
	}	
}