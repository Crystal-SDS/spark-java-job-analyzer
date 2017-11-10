package main.java;

import java.io.IOException;

import org.json.simple.JSONObject;

import main.java.analyzer.FlinkJavaJobAnalyzer;

public class FlinkJavaAnalyzerExecutor {

	public static void main(String[] args) throws IOException {		
        FlinkJavaJobAnalyzer analyzer = new FlinkJavaJobAnalyzer();
        JSONObject result = analyzer.analyze(args[0]);
        System.out.println(result.toString());
	}
}