package test.resources.test_jobs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Stream;

import test.java.cases.TestTask;

public class Java8SimpleLogAnalyzer4 implements TestTask{
	
	public StringBuilder doTask(String inputFile) {
		StringBuilder builder = new StringBuilder();
		try{
			Stream<String> myStream = Files.lines(Paths.get(inputFile));
			long lines = myStream
					.filter(s -> s.contains("Hamlet") && s.contains(" "))
					.map(l -> Arrays.asList(l.split(" ")))
					.map(l -> l.get(0))
					.count();
			
			builder.append(lines);			
			myStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		} 		
		System.out.println(builder.toString());
		return builder;
	}
}