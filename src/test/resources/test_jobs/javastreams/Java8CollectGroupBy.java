package test.resources.test_jobs.javastreams;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import test.java.cases.TestTask;

public class Java8CollectGroupBy  implements TestTask{
	
	public StringBuilder doTask(String inputFile) {
	//public static void main(String[] args) {
		StringBuilder builder = new StringBuilder();
		//2010-01-01T00:00:00+01:00,1470.57,1.47057,0,gas,METER000029,100,Limoges,87,45.839566,1.203017
		/*date,index,sumHC,sumHP,type,vid,size,city,region,lat,lng */
		//date,index,sumHC,sumHP,type,vid,size,city,region,lat,lng
		try{
			Stream<String> myStream = Files.lines(Paths.get(inputFile)).sorted();
			Map<String, Double> result = myStream
							.map(s -> {
								String[] split = s.split(",");
								String slotMeterKey = null;
								try {
									slotMeterKey = String.valueOf(Instant.parse(split[0].substring(0, split[0].indexOf("+"))+"Z")
											.toEpochMilli()/(3600*1000)) + "-" + split[5];
								} catch (Exception e) {	e.printStackTrace();}
								return new SimpleEntry<String, Double>(slotMeterKey, new Double(split[1]));
							})
							.collect(Collectors.groupingBy(SimpleEntry<String, Double>::getKey, 
									 Collectors.reducing(0.0, SimpleEntry<String, Double>::getValue, (a, b) -> a + b)));
			
			for (Entry<String, Double> entry: result.entrySet())
				builder.append(entry.getKey().toString() + "-" + entry.getValue().toString() + "\n");

		} catch (IOException e) {
			e.printStackTrace();
		} 		
		return builder;
	}

}
