package main.java;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.json.simple.JSONObject;

import main.java.analyzer.ArgsPreAnalyzer;

public class JavaArgsPreAnalyzerExecutor {

	public static void main(String[] args) throws IOException {		
		ArgsPreAnalyzer analyzer = new ArgsPreAnalyzer();
		List<String> jobArguments = Arrays.asList(args).subList(1, args.length); 
        JSONObject result = analyzer.preAnalyze(args[0], jobArguments);
        System.out.println(result.toString());
	}
}