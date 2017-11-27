package main.java.analyzer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;

import org.json.simple.JSONObject;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.Node;
import com.github.javaparser.ast.NodeList;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.body.Parameter;
import com.github.javaparser.ast.comments.Comment;
import com.github.javaparser.ast.expr.ArrayAccessExpr;
import com.github.javaparser.ast.expr.Expression;
import com.github.javaparser.ast.expr.IntegerLiteralExpr;
import com.github.javaparser.ast.expr.StringLiteralExpr;
import com.github.javaparser.ast.visitor.GenericVisitorAdapter;
import com.github.javaparser.ast.visitor.ModifierVisitor;

import main.java.utils.Utils;

public class ArgsPreAnalyzer {

	@SuppressWarnings("unchecked")
	public JSONObject preAnalyze(String fileToAnalyze, List<String> mainArguments) {
		
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
        
        //System.out.println(cu.toString());     
        
        //Keep the original job code
        String originalJobCode = Utils.stripSpace(cu.toString());
        String modifiedJobCode = originalJobCode;
        
        String mainArgsParamName = cu.accept(new SearchArgsParamNameVisitor(), "main");
        
        ArrayAccessVisitorArguments arrayAccessArguments = 
        		new ArrayAccessVisitorArguments(mainArgsParamName, mainArguments);
        cu.accept(new ArrayAccessVisitor(), arrayAccessArguments);
        
        modifiedJobCode = cu.toString();
        
        JSONObject obj = new JSONObject();
        obj.put("pushdown-job-code", modifiedJobCode);
        return obj;
        
	}

	private static String getMethodFirstParamName(MethodDeclaration method) {
    	NodeList<Parameter> params = method.getParameters();
    	Parameter argsParam = params.get(0);
    	return argsParam.getNameAsString();
	}
	
	private static class ArrayAccessVisitor extends ModifierVisitor<ArrayAccessVisitorArguments> {
	    @Override
	    public Node visit(ArrayAccessExpr expr, ArrayAccessVisitorArguments arguments) {
	    	String arrayName = arguments.getArrayName();
	    	List<String> replacementArguments = arguments.getReplacementArguments();

	    	if (expr.getName().toString().equals(arrayName)) {
	    		Expression indexExpression = expr.getIndex();
	    		if (indexExpression instanceof IntegerLiteralExpr) {
	    			int index = Integer.valueOf(((IntegerLiteralExpr) indexExpression).getValue());
	    			return new StringLiteralExpr(replacementArguments.get(index));
	    		}
	    		
	    	}
	        return expr;
	    }
	}
	
	private static class SearchArgsParamNameVisitor extends GenericVisitorAdapter<String, String> {
	    @Override
	    public String visit(MethodDeclaration methodDeclaration, String methodName) {
	    	if (methodDeclaration.getNameAsString().equals(methodName)) {
            	return getMethodFirstParamName(methodDeclaration);
            }
			return null;
	    }
	}
	
	private static class ArrayAccessVisitorArguments {
		private String arrayName;
		private List<String> replacementArguments;
		
		public ArrayAccessVisitorArguments(String arrayName, List<String> replacementArguments) {
			this.arrayName = arrayName;
			this.replacementArguments = replacementArguments;
		}

		public String getArrayName() {
			return arrayName;
		}
		
		public List<String> getReplacementArguments() {
			return replacementArguments;
		}
	}
	
}


