val stageMetrics = ch.cern.sparkmeasure.Utils.readSerializedStageMetrics("/your_path/results/stageMetrics.serialized")
stageMetrics.toDF.write.format("com.databricks.spark.csv").option("header", "false").save("/your_path/results/results_stages.csv")
val taskMetrics = ch.cern.sparkmeasure.Utils.readSerializedTaskMetrics("/your_path/results/taskMetrics.serialized")
taskMetrics.toDF.write.format("com.databricks.spark.csv").option("header", "false").save("/your_path/results/results_tasks.csv")
