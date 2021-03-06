package main.java.analyzer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.comments.Comment;

import main.java.analyzer.visitor.StatementsExtractor;
import main.java.analyzer.visitor.StreamIdentifierVisitor;
import main.java.dataset.DatasetTranslation;
import main.java.dataset.translation.sparkjava.RDDTranslator;
import main.java.graph.FlowControlGraph;
import main.java.graph.GraphNode;
import main.java.rules.LambdaRule;
import main.java.rules.reverse.sparkjava.TransformationModificationRuleSpark;
import main.java.utils.Utils;

public class SparkJavaJobAnalyzer extends JavaStreamsJobAnalyzer {

	protected final String jobType = "sparkjava";

	protected static String targetedDatasets = "(RDD|JavaRDD|JavaPairRDD|DStream|"
			+ "JavaDStream|JavaPairDStream)\\s*";
	
	protected final String pushableTransformations = "(map|filter|flatMap|mapToPair|"
			+ "reduceByKey|reduce|distinct|groupByKey)";
	protected final String pushableActions = "(collect|count|foreach)";
	
	protected final String translationRulesPackage = "main.java.rules.translation." + jobType  + ".";
	protected final String reverseRulesPackage = "main.java.rules.reverse." + jobType  + ".";
	
	protected final String translatedFilename = "Java8Translated";
	
	public JSONObject analyze (String fileToAnalyze) {
		
		//Get the input stream from the job file to analyze
		FileInputStream in = null;		
		try {
			in = new FileInputStream(fileToAnalyze);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		//Parse the job file
        CompilationUnit cu = JavaParser.parse(in); 
        for (Comment comment: cu.getAllContainedComments())
        	comment.remove();
                
        //Keep the original job code if we cannot execute lambdas due to resource constraints
        String originalJobCode = Utils.stripSpace(cu.toString());
        String translatedJobCode = originalJobCode;
        
        //First, get all the variables of type Stream, as they are the candidates to push down lambdas
        new StreamIdentifierVisitor(targetedDatasets, identifiedStreams).visit(cu, null);
        
        //Second, once we have the streams identified, we have to inspect each one looking 
        //for safe lambdas to push down      
        new StatementsExtractor(identifiedStreams, pushableTransformations, 
        		pushableActions, null).visit(cu, null); 
        
        //Next, we apply translation rules so the Spark job is understandable by the JavaStreams analyzer
        for (String key: identifiedStreams.keySet()){
        	applyRulesToControlFlowGraph(identifiedStreams.get(key), translationRulesPackage);
        }      
        
        //Translate specific Spark Jobs classes/RDD calls into JavaStreams classes/calls 
        for (String key: identifiedStreams.keySet()){
        	FlowControlGraph graph = identifiedStreams.get(key);  
        	//Instantiate the class and execute the translation to Java8 streams
			DatasetTranslation datasetTranslator = new RDDTranslator();
			translatedJobCode = datasetTranslator.applyDatasetTranslation(
				graph.getRdd(), graph.getType(), translatedJobCode);
        	//Perform the translation for each of the lambdas of the dataset
        	for (GraphNode node: identifiedStreams.get(key)){
        		//Modify the original's job code according to translation rules
        		String replacement = "";
        		if(!node.getCodeReplacement().equals("")) 
        			replacement = "." + node.getCodeReplacement();
    			translatedJobCode = translatedJobCode.replace("." + node.getLambdaSignature(), replacement);
           	}
        }  
        	
        //Create a new file with the job translated into JavaStreams classes and functions
        String className = Paths.get(fileToAnalyze).getFileName().toString().replace(".java", "");
        translatedJobCode = translatedJobCode.replace(className, className + translatedFilename);
        String translatedJobPath = Paths.get(fileToAnalyze.replace(".java", translatedFilename+".java")).toString();
        try (PrintWriter out = new PrintWriter(translatedJobPath)){
            out.println(translatedJobCode);
        } catch (FileNotFoundException e) {
			e.printStackTrace();
		}
        
        //Execute the JavaStreams analyzer on the translated job
        JavaStreamsJobAnalyzer javaStreamsAnalyzer = new JavaStreamsJobAnalyzer();
        JSONObject result = javaStreamsAnalyzer.analyze(translatedJobPath);
        //The lambdas to migrate should be Java8 Stream lambdas, as they will be executed by the Storlet
        Map<String, List<SimpleEntry<String, String>>> lambdasToMigrate = Utils.getLambdasToMigrate(result);
        String modifiedJobCode =  originalJobCode;
        
        //Modify the original job by deleting all the functions that have been moved to the storage
        LambdaRule pushdownLambdaRule = null;
        for (String rddName: identifiedStreams.keySet()){   
        	int pushdownCounter = 0;     	
	        for (GraphNode node: identifiedStreams.get(rddName)){  
	        	if (!lambdasToMigrate.containsKey(rddName)) continue;
	        	String functionName = node.getFunctionName();
	        	for (SimpleEntry<String, String> theLambda: lambdasToMigrate.get(rddName)){
	        		System.out.println(theLambda.getKey() + " > " + theLambda.getValue());
	        		if (node.getCodeReplacement().equals(theLambda.getKey())){
	        			try {
							//Instantiate the class that contains the rules to pushdown a given lambda
							pushdownLambdaRule = (LambdaRule) Class.forName(
									reverseRulesPackage + new String(functionName.substring(0, 1)).toUpperCase() +
									functionName.substring(1, functionName.length())).newInstance();
							pushdownCounter++;
							//We do not infer Spark types, so we use the Java8 types inferred as hints
							node.setFunctionType(theLambda.getValue());
							if (pushdownLambdaRule instanceof TransformationModificationRuleSpark)
								((TransformationModificationRuleSpark)pushdownLambdaRule)
									.setForceConversionMap(pushdownCounter==lambdasToMigrate.get(rddName).size());
							//Get whether the current lambda can be pushed down or not
							pushdownLambdaRule.applyRule(node);
							String codeReplacement = "";
							if (!node.getCodeReplacement().equals(""))
								codeReplacement =  "." + node.getCodeReplacement();
							modifiedJobCode = modifiedJobCode.replace("." + node.getLambdaSignature(), codeReplacement);
						} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
							System.err.println("No rule for lambda: " + functionName + " in " + reverseRulesPackage);
						}
	        		}
	        	}				
	        }		
		}	        
        
        //Finally, we want to pipeline all the lambdas related to a data container, not to a RDD
        //RDD a = "container"
        //a.map()
        //RDD b = a
        //b.filter()
        //
        //Expected result in perContainerLambdasToMigrate: {"container": [a, b]}
        Map<String, List<SimpleEntry<String, String>>> perContainerLambdasToMigrate = new HashMap<>();
        //Note that we iterated a map that keeps the insertion order, which means the order of lambdas
        for (String rddName: identifiedStreams.keySet()) {
        	FlowControlGraph graph = identifiedStreams.get(rddName);
        	String containerName = graph.getOriginContainer();
        	//First entry for a container, instantiate the list of lambdas for it
        	if (containerName!=null && !perContainerLambdasToMigrate.containsKey(containerName))
        		perContainerLambdasToMigrate.put(containerName, new ArrayList<>());
        	//If this is a derived RDD, look for the container that it points to
        	//Add all the lambdas for this container comming from one or many RDDs
        	if (lambdasToMigrate.containsKey(rddName))
        		perContainerLambdasToMigrate.get(containerName).addAll(lambdasToMigrate.get(rddName));
        }
        //The control plane is in Python, so the caller script will need to handle this result
        //and distinguish between the lambdas to pushdown and the code of the job to submit
        return Utils.encodeResponse(originalJobCode, modifiedJobCode, perContainerLambdasToMigrate);
	}
}