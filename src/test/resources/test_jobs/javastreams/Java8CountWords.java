package test.resources.test_jobs.javastreams;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.stream.Stream;

import test.java.cases.TestTask;

public class Java8CountWords implements TestTask{
	
	public StringBuilder doTask(String inputFile) {
		StringBuilder builder = new StringBuilder();
		try{
			Stream<String> myStream = Files.lines(Paths.get(inputFile));

			Optional<Long> count = myStream.map(s -> new Long(s.split(" ").length))
										   .reduce((a, b) -> a + b);	
			
			builder.append(count+"\n");			
			myStream.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		} 
		return builder;
	}
}
