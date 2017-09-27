package main.java.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.AbstractMap.SimpleEntry;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Utils {
	
	static final String LAMBDA_TYPE_AND_BODY_SEPARATOR = "|";
	//TODO: There is a problem using "," when passing lambdas as Storlet parameters, as the
	//Storlet middleware treats every "," as a separation between key/value parameter pairs
	static final String COMMA_REPLACEMENT_IN_PARAMS = "'";
	static final String EQUAL_REPLACEMENT_IN_PARAMS = "$";
	
	/**
	 * This method scans a String to extract P1 and P2 from inputs like xxx<P1, P2>xxx 
	 * @param parameters
	 * @return
	 */
	public static List<String> getParametersFromSignature(String parameters) {
		List<String> result = new ArrayList<>();
		int openBr = 0;
		int inipos = 0, pos = 0;
		while (pos<parameters.length()) {
			if (parameters.charAt(pos)=='<') openBr++;
			if (parameters.charAt(pos)=='>') openBr--;
			if ((parameters.charAt(pos)==',' && openBr==0) || (pos == parameters.length()-1)){
				if (pos == parameters.length()-1) pos++;
				result.add(parameters.substring(inipos, pos));
				inipos = pos+1; //avoid the comma
			}
			pos++;
		}
		return result;	
	}
	
	/**
	 * This method is intended to return to an external program a JSON String response with
	 * both the lambdas to send to the storage and the final version of the job to execute
	 * at the Spark cluster.
	 * 
	 * @param lambdasToMigrate
	 * @param cu
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static JSONObject encodeResponse(String originalJob, String modifiedJob, 
									Map<String, List<SimpleEntry<String, String>>> lambdasToMigrate) {
		JSONObject obj = new JSONObject();
		JSONObject perDataSourceLambdas = new JSONObject();
		
		for (String dataSource: lambdasToMigrate.keySet()){
			JSONArray jsonArray = new JSONArray();
			for (SimpleEntry<String, String> lambda: lambdasToMigrate.get(dataSource)){
				System.out.println(lambda);
				JSONObject lambdaObj = new JSONObject();
				//TODO: The comma replacement is needed as the "," character is reserved in the
				//Storlet middleware to separate key/value pairs
				String lambdaSignature = (lambda.getValue() + LAMBDA_TYPE_AND_BODY_SEPARATOR 
						+ lambda.getKey()).replace(",", COMMA_REPLACEMENT_IN_PARAMS)
										  .replace("=", EQUAL_REPLACEMENT_IN_PARAMS);
				System.err.println(lambdaSignature);
				lambdaObj.put("lambda-type-and-body", lambdaSignature);
				jsonArray.add(lambdaObj); 
			}
			perDataSourceLambdas.put(dataSource, jsonArray);
		}
		//Separator between lambdas and the job source code
		obj.put("original-job-code", originalJob);	
		obj.put("pushdown-job-code", modifiedJob);	
		obj.put("lambdas", perDataSourceLambdas);
		return obj;
	}
	
	public static Map<String, List<SimpleEntry<String, String>>> getLambdasToMigrate(JSONObject json){		
		Map<String, List<SimpleEntry<String, String>>> lambdasToMigrate = new LinkedHashMap<>();
		JSONObject perDataSourceLambdas = (JSONObject) json.get("lambdas");
		for (Object dataSource: perDataSourceLambdas.keySet()) {
			lambdasToMigrate.put((String) dataSource, new ArrayList<>());
			JSONArray jsonArray = (JSONArray) perDataSourceLambdas.get((String) dataSource);
			Iterator<JSONObject> it = jsonArray.listIterator();
			while (it.hasNext()){
				String lambdaSignature = (String) it.next().get("lambda-type-and-body");
				lambdaSignature = lambdaSignature.replace(COMMA_REPLACEMENT_IN_PARAMS, ",")
												 .replace(EQUAL_REPLACEMENT_IN_PARAMS, "=");
				lambdasToMigrate.get((String) dataSource).add(new SimpleEntry<String, String>(
						lambdaSignature.substring(lambdaSignature.indexOf(LAMBDA_TYPE_AND_BODY_SEPARATOR)+1),
						lambdaSignature.substring(0, lambdaSignature.indexOf(LAMBDA_TYPE_AND_BODY_SEPARATOR))));
			} 		
		}
		return lambdasToMigrate;
	}
	
	public static String getModifiedJobCode(JSONObject json){
		return (String) json.get("pushdown-job-code");
	}
	
	//TODO: Modify this to do not work when spaces are within string literals
	public static String stripSpace(String string) {
        StringBuilder result = new StringBuilder();
        boolean lastWasSpace = true;
        for (int i = 0; i < string.length(); i++) {
            char c = string.charAt(i);
            if (Character.isWhitespace(c)) {
                if (!lastWasSpace) {
                    result.append(' ');
                }
                lastWasSpace = true;
            } else {
                result.append(c);
                lastWasSpace = false;
            }
        }
        return result.toString().trim();
    }		
	
	//TODO: At the moment we can work with simple types, Lists and SimpleEntry
	public static String instantiationSignature(String lastParameter, String streamVariable) {
		//This serves for simple times, like Integer or Long
		if (!lastParameter.contains(",") && !lastParameter.contains("<"))
			return instantiatePrimitive(lastParameter, streamVariable);
		//At the moment, only consider simple type parameters like Integer, String or Long
		if (lastParameter.startsWith("Tuple2"))
			return instantiateTuple(lastParameter, streamVariable);
		//If the we have to convert to a list either from primitives or tuples
		if (lastParameter.startsWith("java.util.ArrayList")){
			//String s = "1.0, 2.0";
			String result = "java.util.Arrays.asList(java.util.stream.Stream.of(s.split(\",\")).map(a -> ";
			String innerType = Utils.getParametersFromSignature(lastParameter
								.replace("java.util.ArrayList<", "").replace(">", "")).get(0);
			if (innerType.startsWith("Tuple2")) result += instantiateTuple(innerType, "a");
			else result += instantiatePrimitive(innerType, "a");
			return result += "))";
		}
		
		System.err.println("Problem performing the map to convert the pushded down type "
				+ "into a type necessary for the remaining lambdas in the modified job");
		return "";
	}
	
	private static String instantiatePrimitive(String lastParameter, String streamVariable) {
		return "new " + lastParameter + "(" + streamVariable + ")";
	}

	private static String instantiateTuple(String lastParameter, String streamVariable) {
		List<String> params = Utils.getParametersFromSignature(
				lastParameter.replace("Tuple2<", "").replace(">", ""));
		String result = "new Tuple2<" + params.get(0) +"," + params.get(1)+ ">(";
		int index = 0;
		for (String p: params){
			if (p.equals("java.lang.String")) result += streamVariable + ".split(\"=\")[" + index +"],";
			else result += p + ".valueOf(" + streamVariable + ".split(\"=\")[" + index +"]), ";
			index++;
		}
		System.err.println(result);
		return result.substring(0, result.length()-2) + ")";		
	}
}