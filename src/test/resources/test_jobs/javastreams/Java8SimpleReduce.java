package test.resources.test_jobs.javastreams;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import test.java.cases.TestTask;

public class Java8SimpleReduce implements TestTask{
	
	public StringBuilder doTask(String inputFile) {
		StringBuilder builder = new StringBuilder();
		try{
			Stream<String> myStream = Files.lines(Paths.get(inputFile));
			//FIXME: Now we only support reduce->optional version
			System.out.println(myStream.filter(s -> s.contains("Hamlet"))
								 .map(l -> l.length())
								 .reduce((a, b) -> {
									 if (Integer.valueOf(a) > Integer.valueOf(b)) return a;
									 else return b;}));
								 
			//builder.append(countLines);
			myStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		} 		
		return builder;
	}

}
