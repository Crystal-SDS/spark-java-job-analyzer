package main.java.rules.translation.sparkjava;

import main.java.graph.GraphNode;
import main.java.rules.LambdaRule;

public class Distinct implements LambdaRule {

	@Override
	public void applyRule(GraphNode graphNode) {
		graphNode.setCodeReplacement(graphNode.getLambdaSignature());
	}

}
