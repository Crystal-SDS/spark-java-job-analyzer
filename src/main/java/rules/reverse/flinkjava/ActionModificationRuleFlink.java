package main.java.rules.reverse.flinkjava;

import java.util.List;

import main.java.graph.GraphNode;
import main.java.rules.LambdaRule;
import main.java.utils.Utils;

public class ActionModificationRuleFlink implements LambdaRule{
	
	@Override
	public void applyRule(GraphNode graphNode) {
		//We need a map for the last type prior to the collector/action
		List<String> nodeParams = graphNode.getPreviousNode().getTypeParametersAsList();
		if (!nodeParams.get(nodeParams.size()-1).equals("java.lang.String")){
			String lastParameter = nodeParams.get(nodeParams.size()-1);
			String conversionFunction = "map";
			graphNode.setCodeReplacement(conversionFunction + 
				"(s -> " + Utils.instantiationSignature(lastParameter.trim(), "s") + ")."
					+ graphNode.getLambdaSignature());
		} else graphNode.setCodeReplacement(graphNode.getLambdaSignature());
	}

}