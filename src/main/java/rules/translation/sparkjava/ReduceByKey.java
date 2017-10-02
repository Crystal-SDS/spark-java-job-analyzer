package main.java.rules.translation.sparkjava;

import java.util.List;

import main.java.graph.GraphNode;
import main.java.rules.LambdaRule;
import main.java.utils.Utils;

public class ReduceByKey implements LambdaRule {

	@Override
	public void applyRule(GraphNode graphNode) {
		//The most similar call for a java8 program of this call is a collector
		StringBuilder replacement = new StringBuilder("collect");
		
		//The collector should group by key
		replacement.append("(java.util.stream.Collectors.groupingBy(");
		String tupleType = null;
		GraphNode toExplore = graphNode;
		while (toExplore!=null){
			//FIXME: Do this with regex
			if (toExplore.getCodeReplacement().contains("new SimpleEntry")){
				tupleType = toExplore.getCodeReplacement().substring(
						toExplore.getCodeReplacement().indexOf("SimpleEntry"), 
						toExplore.getCodeReplacement().lastIndexOf(">")+1);
			}
			toExplore = toExplore.getPreviousNode();
		}
		//TODO: If the type is implicit, then look for the type of the RDD
		if (tupleType == null) 
			System.err.println("ERROR: Unknown collector for translation in ReduceByKey: " 
						+ graphNode.getLambdaSignature());
		
		replacement.append(tupleType + "::getKey, ");
		//Infer the correct built-in collector from the specified function
		String collector = null;
		String reduceFunction = graphNode.getLambdaSignature()
										 .substring(graphNode.getLambdaSignature().indexOf("->"));
		reduceFunction = reduceFunction.substring(0, reduceFunction.lastIndexOf(")"));
		
		collector = "java.util.stream.Collectors.reducing("; 
		List<String> tupleParams = Utils.getParametersFromSignature(
				tupleType.substring(tupleType.indexOf("<")+1, tupleType.lastIndexOf(">")));
		String initialValue = "new " + tupleParams.get(1) + "(0)";
		
		//FIXME: Here are some conditions to infer the initial value for the Collectors.reducing
		//operation, which is necessary for the Streams API. Note that this this only handles a 
		//small subset of cases, but it quite infeasible to infer the correct initial value without 
		//the input of the programmer. This is due to the operational mismatch of Spark.reduceByKey 
		//and Streams.collect + groupingByKey/collecting 
		if (reduceFunction.matches("->\\s*\\(?\\w*\\s*\\+\\s*\\w*\\s*\\)?") ||
				reduceFunction.matches("->\\s*Math.addExact\\(\\s*\\w*\\s*,\\s*\\w*\\s*\\)") ||
					reduceFunction.matches("->\\s*\\(?\\w*\\s*\\-\\s*\\w*\\s*\\)?") ||
						reduceFunction.matches("->\\s*Math.subtractExact\\(\\s*\\w*\\s*,\\s*\\w*\\s*\\)"))
			initialValue = "new " + tupleParams.get(1) + "(0)";
		else if (reduceFunction.matches("->\\s*\\(?\\w*\\s*\\\\*\\s*\\w*\\s*\\)?") ||
				reduceFunction.matches("->\\s*Math.multiplyExact\\(\\s*\\w*\\s*,\\s*\\w*\\s*\\)"))
			initialValue = "new " + tupleParams.get(1) + "(1)";
		else if (reduceFunction.matches("->\\s*Math.min\\(\\s*\\w*\\s*,\\s*\\w*\\s*\\)"))
			initialValue = tupleParams.get(1) + ".MAX_VALUE";
		else if (reduceFunction.matches("->\\s*Math.max\\(\\s*\\w*\\s*,\\s*\\w*\\s*\\)")) 
			initialValue = tupleParams.get(1) + ".MIN_VALUE";
		else if (tupleParams.get(1).equals("String"))
			initialValue = "";
		else System.err.println("WARNING: No initial value found for expression");
			
		collector += initialValue + ", " + tupleType + "::getValue, " + graphNode.getLambdaSignature()
																.substring(graphNode.getLambdaSignature().indexOf("(")+1) ;
		//if (collector == null) 
		//	System.err.println("ERROR: Unknown collector for translation in ReduceByKey: " + reduceFunction);
		
		replacement.append(collector + "))");
		graphNode.setCodeReplacement(replacement.toString());
	}
}