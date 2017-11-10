package test.resources.test_jobs.flinkjava;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.ReduceOperator;

public class FlinkJavaCountWords { 

	public static void main(String[] args) throws Exception {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> text = env.readTextFile("swift2d://wikipedia_en_large.lvm/*");

		text.map(s ->  new Long(s.split(" ").length)).reduce((a, b) -> a + b).print();
	}
}