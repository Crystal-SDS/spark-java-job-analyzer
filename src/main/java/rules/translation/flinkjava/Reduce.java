package main.java.rules.translation.flinkjava;

import main.java.graph.GraphNode;
import main.java.rules.LambdaRule;

public class Reduce implements LambdaRule {

	@Override
	public void applyRule(GraphNode graphNode) {
		graphNode.setCodeReplacement(graphNode.getLambdaSignature());
	}

}
