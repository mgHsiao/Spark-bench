#!/bin/bash
#Description: start spark apps

current_dir=$(cd $(dirname "$0"); pwd)
cd ${current_dir}

#需修改
code="/xxx/code/spark-bench"
spark_path="/xxx/spark/spark-3.0.0-bin-hadoop3.2"
hadoop_path="/xxx/hadoop/hadoop-3.2.1"

hdfs_master="hdfs://x.x.x.x:9000/HiBench/data"
spark_master="spark://x.x.x.x:7077"

train_data=("100G" "200G" "500G" "800G" "1T" "1.5T" "2T" "3T")
algs=("DenseKMeans")

# algs=(SparseNaiveBayes DenseKMeans SVMWithSGDExample)

##
hadoop_mr_example=hadoop-mapreduce-examples-3.2.1.jar
jars=spark-bench-0.0.1-SNAPSHOT-jar-with-dependencies.jar
hadoop_auto_gen=autogen-1.0-SNAPSHOT-jar-with-dependencies.jar

declare -A algs_dict
algs_dict["ScalaHiveSQL"]="HiveSQL, sql, hive"
algs_dict["SVMWithSGDExample"]="SVM, ml"
algs_dict[SparseNaiveBayes]="Bayes, ml"
algs_dict[DenseKMeans]="Kmeans, ml"
algs_dict[NWeight]="NWeight, graph"

#删除 hdfs 中的数据
for(( j=0;j<${#algs[@]};j++)) do

	${hadoop_path}/bin/hadoop fs -rm -r -skipTrash ${hdfs_master}/HiBench/${Algs[j]}

done

declare -A svm_data
#examples,features
svm_data["10G"]="30000, 50000"
svm_data["30G"]="45000, 100000"
svm_data["50G"]="70000, 100000"
svm_data["70G"]="50000, 200000"
svm_data["100G"]="72000, 200000"
svm_data["500G"]="230000, 300000"
svm_data["800G"]="360000, 300000"
svm_data["1T"]="450000, 300000"
svm_data["1.5T"]="550000, 400000"
svm_data["2T"]="720000, 400000"
svm_data["3T"]="1200000, 400000"

declare -A bayes_data
#page,classes, ngram
bayes_data["10G"]="500000, 100, 2"
bayes_data["30G"]="1000000, 100, 2"
bayes_data["50G"]="20000000, 20000, 2"

bayes_data["10G"]="3000000, 1000, 2"
bayes_data["30G"]="9000000, 1000, 2"
bayes_data["50G"]="15000000, 2000, 2"
bayes_data["70G"]="22000000, 3000, 2"
bayes_data["100G"]="30000000, 5000, 2"
bayes_data["200G"]="60000000, 10000, 2"
bayes_data["500G"]="150000000, 20000, 2"
bayes_data["800G"]="240000000, 50000, 2"
bayes_data["1T"]="300000000, 50000, 2"
bayes_data["1.5T"]="450000000, 80000, 2"
bayes_data["2T"]="600000000, 80000, 2"
bayes_data["3T"]="900000000, 80000, 2"

declare -A kmeans_data
#num_of_clusters, dimensions, num_of_samples, samples_per_inputfile, max_iteration, k, convergedist
kmeans_data["10G"]="5, 20, 55000000, 20000000, 5, 10, 0.5"
kmeans_data["30G"]="5, 20, 170000000, 20000000, 5, 10, 0.5"
kmeans_data["50G"]="5, 20, 270000000, 40000000, 5, 10, 0.5"
kmeans_data["70G"]="5, 20, 385000000, 40000000, 5, 10, 0.5"
kmeans_data["100G"]="5, 20, 550000000, 40000000, 5, 10, 0.5"
kmeans_data["200G"]="5, 20, 1100000000, 40000000, 5, 10, 0.5"
kmeans_data["500G"]="5, 20, 2700000000, 40000000, 5, 10, 0.5"
kmeans_data["800G"]="5, 20, 4400000000, 40000000, 5, 10, 0.5"
kmeans_data["1T"]="5, 20, 5600000000, 40000000, 5, 10, 0.5"
kmeans_data["1.5T"]="5, 20, 8300000000, 40000000, 5, 10, 0.5"
kmeans_data["2T"]="5, 20, 12000000000, 40000000, 5, 10, 0.5"
kmeans_data["3T"]="5, 20, 17000000000, 40000000, 5, 10, 0.5"

function ml_func(){
	local ml_alg=$1
	local gen_jar=$2
	local ml_data_size=$3
	local input_dir=$4
	local output_dir=$5
	local base_hdfs_dir=$6
	local is_gen_data=$7
	local tmp_spark_home=$8

	tmp_spark_home=$spark_path

	local job_option=$input_dir
	local job_class=""
	local is_mr_gen=0
	local gen_class=""
	local gen_option=""
	if [[ $ml_alg = "SVMWithSGDExample" ]]; then
		numIterations=100
		stepSize=1.0
		regParam=0.01
		svm_data_unit=(${svm_data[$ml_data_size]//,/})

		gen_class=cn.nsccgz.spark.bench.ml.SVMDataGenerator
		gen_option=" $input_dir \
				${svm_data_unit[0]} \
				${svm_data_unit[1]} "
		is_mr_gen=0

		job_class="cn.nsccgz.spark.bench.ml.SVMWithSGDExample"
		job_option="--numIterations $numIterations \
			--stepSize $stepSize \
			--regParam $regParam \
			$input_dir "

	elif [[ $ml_alg = "SparseNaiveBayes" ]]; then

		bayes_data_unit=(${bayes_data[$ml_data_size]//,/})
		local OPTION="-t bayes \
        -b ${base_hdfs_dir} \
        -n Input \
        -m 512 \
        -r 256 \
        -p ${bayes_data_unit[0]} \
        -class ${bayes_data_unit[1]} \
        -o sequence"

		gen_class=HiBench.DataGen
		gen_option=${OPTION}
		is_mr_gen=1
		job_class="org.apache.spark.examples.mllib.SparseNaiveBayes "

	elif [[ $ml_alg = "DenseKMeans" ]]; then
		local kmeans_data_unit=(${kmeans_data[$ml_data_size]//,/})

		local storage_level="MEMORY_ONLY"
		local INPUT_CLUSTER=$input_dir/cluster
		local INPUT_SAMPLE=$input_dir/samples
		local OPTION="-sampleDir ${INPUT_SAMPLE} -clusterDir ${INPUT_CLUSTER} \
			-numClusters ${kmeans_data_unit[0]}\
			-numSamples ${kmeans_data_unit[2]}\
			-samplesPerFile ${kmeans_data_unit[3]}\
			-sampleDimension ${kmeans_data_unit[1]}"
		
		gen_class="org.apache.mahout.clustering.kmeans.GenKMeansDataset -D hadoop.job.history.user.location=${INPUT_SAMPLE}"
		gen_option=${OPTION}
		is_mr_gen=1

		input_dir=$input_dir/samples
		job_class="cn.nsccgz.spark.bench.ml.DenseKMeans"
		job_option="-k ${kmeans_data_unit[5]} \
			--numIterations ${kmeans_data_unit[4]} \
			--storageLevel $storage_level \
			$input_dir"
	fi

	if [[ $is_gen_data == true ]]; then
	
		echo "Generate data..."

		if [[ $is_mr_gen = 1 ]]; then
			${hadoop_path}/bin/hadoop  jar  $code/$hadoop_auto_gen \
			$gen_class \
			$gen_option
		else 
			"${tmp_spark_home}/bin/spark-submit"  \
					--class $gen_class \
					--master $spark_master  \
					$gen_jar \
					$gen_option
		fi
	fi	
	
	${hadoop_path}/bin/hadoop fs -du -s -h $input_dir

	echo "Run job ..."
	"${tmp_spark_home}/bin/spark-submit"  \
			--class $job_class \
			--master $spark_master  \
			"${code}/${jars}" \
			$job_option


}

for(( j=0;j<${#algs[@]};j++)) do
	
	alg=${algs[j]}
	alg_conf=(${algs_dict[$alg]//,/})
	Alg=${alg_conf[0]}

	prefix=${alg_conf[1]}
	gen_type=${alg_conf[2]}
	
	for(( i=0;i<${#train_data[@]};i++)) do
			
		input_dir=${hdfs_master}/HiBench/${Alg}/Input
		base_hdfs_dir=${hdfs_master}/HiBench/${Alg}
		output_dir=${hdfs_master}/HiBench/${Alg}/Output

		mr_data_jar=${hadoop_path}/share/hadoop/mapreduce/$hadoop_mr_example

		is_gen_data=true
		ml_func $alg "${code}/${jars}" ${train_data[i]} $input_dir $output_dir $base_hdfs_dir $is_gen_data

		${hadoop_path}/bin/hdfs dfsadmin -safemode leave
		${hadoop_path}/bin/hadoop fs -rm -r -skipTrash ${hdfs_master}/HiBench/${Algs[j]}

	done;
done;

exit 0
