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

# algs=(ScalaTeraSort ScalaWordCount ScalaSort ScalaPageRank)

train_data=("300G" "500G" "800G" "1T" "1.5T" "2T" "3T")
algs=(ScalaTeraSort ScalaWordCount ScalaSort)

# For PageRank
#train_data=("70G" "90G" "128G" "200G" "300G")
# algs=("ScalaPageRank")

##
hadoop_mr_example=hadoop-mapreduce-examples-3.2.1.jar
jars=spark-bench-0.0.1-SNAPSHOT-jar-with-dependencies.jar
hadoop_auto_gen=autogen-1.0-SNAPSHOT-jar-with-dependencies.jar

declare -A micro_data
micro_data=([10G]=10000000000 [30G]=30000000000 [50G]=50000000000 [70G]=70000000000 [90G]=90000000000 [128G]=128000000000)
micro_data["200G"]=200000000000
micro_data["300G"]=300000000000
micro_data["500G"]=540000000000
micro_data["800G"]=850000000000
micro_data["1T"]=1000000000000
micro_data["1.2T"]=1200000000000
micro_data["1.5T"]=1500000000000
micro_data["2T"]=2000000000000
micro_data["2.5T"]=2500000000000
micro_data["3T"]=3000000000000

#page rank
micro_data[pr10G]=15000000
micro_data[pr30G]=45000000
micro_data[pr50G]=75000000
micro_data[pr70G]=110000000
micro_data["pr90G"]=150000000
micro_data["pr128G"]=200000000
micro_data["pr200G"]=300000000
micro_data["pr300G"]=400000000
micro_data["pr500G"]=700000000
micro_data["pr800G"]=1100000000
micro_data["pr1T"]=1500000000
micro_data["pr1.2T"]=1800000000
micro_data["pr1.5T"]=2200000000
micro_data["pr2T"]=3000000000
micro_data["pr2.5T"]=3700000000
micro_data["pr3T"]=4500000000
micro_data["pr3.5T"]=5200000000

declare -A algs_dict
algs_dict["ScalaTeraSort"]="Terasort, micro, teragen"
algs_dict["ScalaWordCount"]="Wordcount, micro, randomtextwriter"
algs_dict["ScalaPageRank"]="Pagerank, websearch, pr"
algs_dict["ScalaSort"]="Sort, micro, randomtextwriter"

#删除 hdfs 中的数据
for(( j=0;j<${#algs[@]};j++)) do

	${hadoop_path}/bin/hadoop fs -rm -r -skipTrash ${hdfs_master}/HiBench/${Algs[j]}

done


function micro_func(){
	local is_gen_data=$1
	local micro_alg=$2
	local gen_jar=$3
	local micro_data_size=$4
	local gen_type=$5
	local input_dir=$6
	local output_dir=$7
	local PAGERANK_BASE_HDFS=$8
	# echo "$@"

	local tmp_spark_home=$spark_path

	if [[ $is_gen_data == true ]] 
		then 
			echo "start generate data...... ${gen_type}"
			if [[ ${gen_type} = "teragen" ]]
			then
				tera_size=`expr ${micro_data[$micro_data_size]} / 100 `
				${hadoop_path}/bin/hadoop  jar \
					$gen_jar ${gen_type} \
					-D mapreduce.job.maps=512 \
					-D mapreduce.job.reduces=256 \
					$tera_size \
					$input_dir

			elif [[ ${gen_type} = "randomtextwriter" ]]
			then


				${hadoop_path}/bin/hadoop  jar \
					$gen_jar ${gen_type} \
					-D mapreduce.randomtextwriter.totalbytes=${micro_data[$micro_data_size]}  \
					-D mapreduce.randomtextwriter.bytespermap=50000000 \
					-D mapreduce.job.maps=512 \
					-D mapreduce.job.reduces=256 \
					$input_dir
			
			elif [[ ${micro_alg} = "ScalaPageRank" ]]
			then
				page_size="${micro_data[pr${micro_data_size}]}"
				pr_option="-t pagerank \
				-b $PAGERANK_BASE_HDFS \
				-n Input \
				-m 512 \
				-r 256 \
				-p ${page_size} \
				-pbalance -pbalance \
				-o text"
				
				${hadoop_path}/bin/hadoop  jar  $code/$hadoop_auto_gen \
					HiBench.DataGen ${pr_option}

			fi
			
		fi
		if [[ ${micro_alg} = "ScalaPageRank" ]]
		then 
			input_dir=$input_dir/edges
			option="3"
		fi
		${hadoop_path}/bin/hadoop fs -du -s -h $input_dir

		echo "start runing job..."
		echo ${micro_alg}
	
		"${tmp_spark_home}/bin/spark-submit"  \
			--class "cn.nsccgz.spark.bench.${micro_alg}" \
			--master $spark_master  \
			"${code}/${jars}" \
			$input_dir \
			$output_dir \
			$option


}


for(( j=0;j<${#algs[@]};j++)) do
	
	
	alg=${algs[j]}
	alg_conf=(${algs_dict[$alg]//,/})
	Alg=${alg_conf[0]}

	gen_type=${alg_conf[2]}

	
	for(( i=0;i<${#train_data[@]};i++)) do
			
		input_dir=${hdfs_master}/HiBench/${Alg}/Input
		base_hdfs_dir=${hdfs_master}/HiBench/${Alg}
		output_dir=${hdfs_master}/HiBench/${Alg}/Output

		mr_data_jar=${hadoop_path}/share/hadoop/mapreduce/$hadoop_mr_example

		is_gen_data=true
		micro_func $is_gen_data ${algs[j]} $mr_data_jar ${train_data[i]} $gen_type $input_dir $output_dir $base_hdfs_dir

		${hadoop_path}/bin/hdfs dfsadmin -safemode leave
		# 磁盘容量够大，可以不删除
		${hadoop_path}/bin/hadoop fs -rm -r -skipTrash ${hdfs_master}/HiBench/${Algs[j]}

	done;
done;

exit 0
