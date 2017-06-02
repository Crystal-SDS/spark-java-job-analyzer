#Execute 5 experiments per configuration
i="0"
while [ $i -lt 5 ]
do
	#We assume that you have the executor file from the job analyzer project source https://github.com/Crystal-SDS/spark-java-job-analyzer
    cd ~/your_path/spark-java-job-analyzer/src/main/resources
    #Execute the executable jar from the job analyzer project (you can do it with Eclipse, for instance) and select the job to run
	python SparkJavaJobAnalyzerExecutor.py  ~/raul/SparkJavaJobAnalyzer.jar ~/raul/SparkJavaSimpleTextAnalysis2.java 
    #Wait some time until you execution finished (change this value depending on the duration of your jobs!)
    echo "Waiting 150 seconds to execute next experiment..."
	sleep 150
	echo "Collecting results..."
	#Collect the results after each execution with the collect_metrics.sh script
	cd ~/your_path
	/bin/bash collect_metrics.sh
	mv results_package.tar.gz results_package$i.tar.gz
	rm -rf results

i=$[$i+1]
done

