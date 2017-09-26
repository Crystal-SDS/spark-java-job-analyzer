package test.java.cases.javastreams;

import test.java.AbstractAnalyzerTest;
import test.resources.test_jobs.javastreams.Java8StreamVariableAssignments;

public class Java8StreamVariableAssignmentsTest extends AbstractAnalyzerTest{	
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		this.inputStorletFile = "test_data/hamlet.txt";
		this.analyticsJob = new Java8StreamVariableAssignments();
		this.jobToAnalyze = "/test_jobs/javastreams/Java8StreamVariableAssignments.java";
	}	
}