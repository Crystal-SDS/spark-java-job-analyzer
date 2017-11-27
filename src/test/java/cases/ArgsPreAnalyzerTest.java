package test.java.cases;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;

import junit.framework.TestCase;
import main.java.analyzer.ArgsPreAnalyzer;
import main.java.utils.Utils;

public class ArgsPreAnalyzerTest extends TestCase{	
	
	protected final String TEST_PATH = Paths.get("").toAbsolutePath().toString()+
			"/src/test/resources/";
	
	public void testJob() {
    
		List<String> externalArguments = new ArrayList<>();
		externalArguments.add("swift2d://data99.lvm/largefile.csv");
		externalArguments.add("SparkJavaCountWordsWithArgs");
		
		ArgsPreAnalyzer argsPreAnalyzer = new ArgsPreAnalyzer();        
        // visit and print the methods names
        JSONObject pushdownAnalysisResult = 
        argsPreAnalyzer.preAnalyze(
        		this.TEST_PATH + "/test_jobs/sparkjava/SparkJavaCountWordsWithArgs.java",
        		externalArguments);
        
        /*System.out.println("LAMBDAS TO MIGRATE FROM SPARK:");
        System.out.println(Utils.getLambdasToMigrate(pushdownAnalysisResult));*/
        System.out.println("MODIFIED SPARK JOB:");
        System.out.println(Utils.getModifiedJobCode(pushdownAnalysisResult));
	}

}
