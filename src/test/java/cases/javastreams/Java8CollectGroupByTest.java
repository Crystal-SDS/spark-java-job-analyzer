package test.java.cases.javastreams;

import test.java.AbstractAnalyzerTest;
import test.resources.test_jobs.javastreams.Java8CollectGroupBy;

public class Java8CollectGroupByTest extends AbstractAnalyzerTest{	
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		this.inputStorletFile = "test_data/meter-new-15MB.csv";
		this.analyticsJob = new Java8CollectGroupBy();
		this.jobToAnalyze = "/test_jobs/javastreams/Java8CollectGroupBy.java";
	}	
}