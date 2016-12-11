## Hadoop
* **env**

		export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
		export HADOOP_PREFIX=/home/seaf/app/hadoop-2.7.3
		export HADOOP_HOME="$HADOOP_PREFIX"

* **yarn-site.xml**
		
		yarn.resourcemanager.hostname = hz09
		yarn.resourcemanager.webapp.address = ${yarn.resourcemanager.hostname}:8088
		
		# The minimum allocation for every container request at the RM, in terms of virtual CPU cores. Requests lower than this will throw a InvalidResourceRequestException.
		yarn.scheduler.maximum-allocation-vcores = 32    
		
		# Number of vcores that can be allocated for containers. This is used by the RM scheduler when allocating resources for containers. This is not used to limit the number of physical cores used by YARN containers.	
		yarn.nodemanager.resource.cpu-vcores = 8  

		# The maximum allocation for every container request at the RM, in MBs. Memory requests higher than this will throw a InvalidResourceRequestException.
		yarn.scheduler.maximum-allocation-mb = 8192   

		# Amount of physical memory, in MB, that can be allocated for containers.
		yarn.nodemanager.resource.memory-mb = 8192  



* **core-site.xml**

		hadoop.tmp.dir = /tmp/hadoop-${user.name}


* **hdfs-site.xml**
		
		dfs.namenode.http-address = hz09:50070
		dfs.datanode.http.address = hz09:50075

* **cmd**	

		$ hdfs namenode -format
		$ ./sbin/start-dfs.sh
		$ ./sbin/start-yarn.sh
		$ jps (check process)
		$ hadoop jar ${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar  wordcount /data/Human /output/wordcount

* [tutorial](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html)
* [example](http://www.powerxing.com/install-hadoop-cluster/)


## Spark

* **spark-env.sh** [link](http://www.cnblogs.com/hseagle/p/4052572.html)  

	最主要的是指定ip地址，如果运行的是master（standalone模式），就需要指定SPARK_MASTER_IP，如果准备运行driver或worker就需要指定SPARK_LOCAL_IP（cluster，client模式），要和本机的IP地址一致，否则启动不了。

		SPARK_HOME=/home/seaf/app/spark-2.0.2-bin-hadoop2.7
		SPARK_LOCAL_IP=127.0.0.1
		SPARK_MASTER_IP=127.0.0.1



* **spark-defaults.conf** [link](http://www.cnblogs.com/hseagle/p/4052572.html)  

  编辑driver所在机器上的spark-defaults.conf，该文件会影响 到driver所提交运行的application，及专门为该application提供计算资源的executor的启动参数。

		spark.serializer                 org.apache.spark.serializer.KryoSerializer
		
		# used in cluster mode.
		# spark.driver.memory              512m
		
		spark.executor.memory            512m
		spark.executor.instances          1
		


* **cluster application**
             
      $ ./bin/spark-submit --class org.apache.spark.examples.SparkPi \
		--master yarn \
		--deploy-mode cluster \
		--driver-memory 4g \
		--executor-memory 2g \
		--executor-cores 1 \
		--queue default  \ 
		./examples/jars/spark-examples_2.11-2.0.2.jar \
		10

* **add other jars**

      $ ./bin/spark-submit --class my.main.Class \
      	--master yarn \
	    --deploy-mode cluster \
	    --jars my-other-jar.jar,my-other-other-jar.jar \
    	my-main-jar.jar \
	    app_arg1 app_arg2

* **client mode**

	可用在集群环境中单步执行，资源由spark-defaults.conf指定或命令参数。
          
      ./bin/spark-shell --master yarn --deploy-mode client

* [running-on-yarn](http://spark.apache.org/docs/latest/running-on-yarn.html)  
  [spark-standalone](http://spark.apache.org/docs/latest/spark-standalone.html)  
  [config](http://spark.apache.org/docs/latest/spark-standalone.html)

* **error**  
[cllosed channel exceptions](http://stackoverflow.com/questions/39467761/how-to-know-what-is-the-reason-for-closedchannelexceptions-with-spark-shell-in-y)  
[service 'sparkDriver' failed after 16 retries](http://stackoverflow.com/questions/29906686/failed-to-bind-to-spark-master-using-a-remote-cluster-with-two-workers)





