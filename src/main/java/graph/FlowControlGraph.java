package main.java.graph;

import java.util.Iterator;

/**
 * This graph represents the sequence of operations executed in a single RDD.
 * Each operation is represented as a {@link GraphNode} object.
 * 
 * @author Raul Gracia
 *
 */
public class FlowControlGraph implements Iterable<GraphNode>{
	
	private String rdd = "";
	private String type = "";
	
	/*Actual data source for this RDD (e.g., Swift data container)*/
	private String originContainer;
	/*If an RDD comes from a transformation from another one, point to it*/
	private FlowControlGraph originRDD;
	
	private GraphNode root;
	
	private GraphNode lastNode;

	public FlowControlGraph(String rdd) {
		this.rdd = rdd;
	}
	
	/**
	 * Add a new node to the flow control graph.
	 * 
	 * @param operation
	 * @param pushable
	 */
	public void appendOperationToRDD(String operation, String functionType, boolean terminal){
		GraphNode toAdd = new GraphNode();
		toAdd.setTerminal(terminal);
		toAdd.setLambdaSignature(operation);
		toAdd.setFunctionType(functionType);
		toAdd.setMyRDD(this);
		if (root==null) {
			root = lastNode = toAdd;
		}else{
			lastNode.setNextNode(toAdd);
			toAdd.setPreviousNode(lastNode);
			lastNode = toAdd;
		}
	}
	
	@Override
	public Iterator<GraphNode> iterator() {
		return new FlowControlGraphIterator(this);
	} 
	
	public String toString(){
		StringBuilder output = new StringBuilder("Graph for RDD: " + this.rdd + "\n{\n ");
		boolean finishLoop = root == null;
		GraphNode pointer = root;
		while (!finishLoop){
			output.append(pointer.getLambdaSignature() + " -> type of lambda: " + pointer.getFunctionType()
					+ ". Is terminal? " + pointer.isTerminal());
			output.append("\n ^ \n");
			pointer = pointer.getNextNode();
			finishLoop = pointer == null;
		}
		output.append("\n}");
		return output.toString();		
	}
	
	/*Access methods*/
	
	public String getRdd() {
		return rdd;
	}

	public void setRdd(String rdd) {
		this.rdd = rdd;
	}

	public GraphNode getRoot() {
		return root;
	}

	public void setRoot(GraphNode root) {
		this.root = root;
	}

	public FlowControlGraph getOriginRDD() {
		return originRDD;
	}

	public void setOriginRDD(FlowControlGraph originRDD) {
		this.originRDD = originRDD;
	}

	public boolean isDerivedRDD() {
		return originRDD!=null;
	}
	
	public GraphNode getLastNode() {
		return lastNode;
	}

	public void setLastNode(GraphNode lastNode) {
		this.lastNode = lastNode;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getOriginContainer() {
		return (originContainer!=null)? originContainer : originRDD.getOriginContainer();
	}

	public void setOriginContainer(String oiriginContainer) {
		this.originContainer = oiriginContainer;
	}
	
}

class FlowControlGraphIterator implements Iterator<GraphNode>{
	
	private GraphNode pointer;

	public FlowControlGraphIterator(FlowControlGraph flowControlGraph) {
		this.pointer = flowControlGraph.getRoot();
	}

	@Override
	public boolean hasNext() {
		return pointer!=null;
	}

	@Override
	public GraphNode next() {
		GraphNode current = pointer;
		pointer = pointer.getNextNode();
		return current;
	}
	
}
