package test.java.cases.flinkjava;

import java.nio.file.Paths;

import org.json.simple.JSONObject;

import junit.framework.TestCase;
import main.java.analyzer.FlinkJavaJobAnalyzer;
import main.java.utils.Utils;

public class FlinkJavaCountWordsTest extends TestCase{	
	
	protected final String TEST_PATH = Paths.get("").toAbsolutePath().toString()+
			"/src/test/resources/";
	
	public void testJob() {
		/*
		 * STEP 2: Execute pushdown analysis on the analytics task
		 */     
		FlinkJavaJobAnalyzer jobAnalyzer = new FlinkJavaJobAnalyzer();        
        // visit and print the methods names
        JSONObject pushdownAnalysisResult = jobAnalyzer.analyze(
        		this.TEST_PATH + "/test_jobs/flinkjava/FlinkJavaCountWords.java");
        
        System.out.println("LAMBDAS TO MIGRATE FROM FLINK:");
        System.out.println(Utils.getLambdasToMigrate(pushdownAnalysisResult));
        System.out.println("MODIFIED FLINK JOB:");
        System.out.println(Utils.getModifiedJobCode(pushdownAnalysisResult));
	}	
}
