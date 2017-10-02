package test.java.cases.javastreams;

import test.java.AbstractAnalyzerTest;
import test.resources.test_jobs.javastreams.Java8CountWords;

public class Java8CountWordsTest extends AbstractAnalyzerTest{	
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		this.inputStorletFile = "test_data/1_AA_wiki_00";
		this.analyticsJob = new Java8CountWords();
		this.jobToAnalyze = "/test_jobs/javastreams/Java8CountWords.java";
	}	
}