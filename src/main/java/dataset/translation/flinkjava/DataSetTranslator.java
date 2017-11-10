package main.java.dataset.translation.flinkjava;

import main.java.dataset.DatasetTranslation;

public class DataSetTranslator implements DatasetTranslation {
	
	private final static String flinkAPIPackage = "org.apache.flink.api.java.";
	
	public String applyDatasetTranslation(String datasetName, String datasetType, String jobCode) {
		String mainType = datasetType;
		String streamType = "Stream";
		if (mainType.contains("<")) 
			mainType = mainType.substring(0, mainType.indexOf("<"));
		//Change any possible reference to the JavaRDD class by Stream
		jobCode = jobCode.replace(flinkAPIPackage + mainType, "java.util.stream.Stream");
		//Change the type declaration of the variable		
		String newDataseType = datasetType.replace(mainType, streamType);
		newDataseType = newDataseType.replace("Tuple2", "SimpleEntry");
		jobCode = jobCode.replace("scala.Tuple2", "java.util.AbstractMap.SimpleEntry");
		return jobCode.replace(datasetType + " " + datasetName, newDataseType + " " + datasetName);
	}
}
