package main.java.rules.reverse.sparkjava;

import java.util.List;

import main.java.graph.GraphNode;
import main.java.rules.LambdaRule;
import main.java.utils.Utils;

public class TransformationModificationRuleSpark implements LambdaRule{
	
	private boolean forceConversionMap = false;
	
	@Override
	public void applyRule(GraphNode graphNode) {
		if (graphNode.getNextNode()!=null && !forceConversionMap) {
			graphNode.setCodeReplacement("");
		}else{
			//We need a map for the last type prior to the collector/action
			List<String> nodeParams = graphNode.getTypeParametersAsList();
			if (!nodeParams.get(nodeParams.size()-1).equals("java.lang.String")){
				String lastParameter = nodeParams.get(nodeParams.size()-1);
				String conversionFunction = "map";
				if (graphNode.getMyRDD().getType().startsWith("JavaPairRDD") ||
						graphNode.getMyRDD().getType().startsWith("JavaPairDStream")) 
					conversionFunction = "mapToPair";
				graphNode.setCodeReplacement(conversionFunction + 
					"(s -> " + Utils.instantiationSignature(lastParameter.trim(), "s") + ")");
			} else graphNode.setCodeReplacement("map(s -> s)");
		}
	}	

	public boolean isForceConversionMap() {
		return forceConversionMap;
	}

	public void setForceConversionMap(boolean forceConversionMap) {
		this.forceConversionMap = forceConversionMap;
	}
	
}