password="put something here"
username="put something here"

mkdir ~/path_to_store_results/results/

#Collect results from 4 nodes at most
i="0"
while [ $i -lt 4 ]
do
	sshpass -p "$password" scp $username@BASE_IP_OF_SPARK_WORKERS$i:/tmp/*.serialized ~/path_to_store_results/results/

i=$[$i+1]
done

#Get the execution metrics from Spark nodes coming from https://github.com/LucaCanali/sparkMeasure and parse them
cat ~/path_to_store_results/export_csv.scala | ~/SPARK_PATH/bin/spark-shell 

#Results can be multiple csv files, so we concat them
cat ~/path_to_store_results/results/results_tasks.csv/part*.csv > ~/path_to_store_results/results/results_tasks_all.csv
cat ~/path_to_store_results/results/results_stages.csv/part*.csv > ~/path_to_store_results/results/results_stages_all.csv

#Create a results tar file
tar -zcvf ~/path_to_store_results/results_package.tar.gz ~/path_to_store_results/results

#Delete possible results container from job execution, so the next execution can start
source ~/path_to_store_results/swift-openrc.sh
swift delete pushdown_test

#Remove metrics from previous execution from Spark nodes
i="0"
while [ $i -lt 4 ]
do
        sshpass -p "$password" ssh $username@BASE_IP_OF_SPARK_WORKERS$i 'rm /tmp/*.serialized'

i=$[$i+1]
done

