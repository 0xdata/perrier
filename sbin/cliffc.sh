#!/bin/bash
export SPARK_MASTER_IP="localhost"
export SPARK_MASTER_PORT="7077"
export SPARK_WORKER_PORT="7087"
export SPARK_WORKER_INSTANCES=1
export MASTER="spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT"
echo .
echo .
echo ============================
echo .   MASTER
echo .
./start-master.sh 
echo .
echo .
echo ============================
echo .   WORKER
echo .
./start-slave.sh 1 $MASTER

# launch spark shell
echo .
echo .
echo ============================
echo .   SHELL
echo .
../bin/spark-shell
#CLASSPATH=".;C:\users\cliffc\Desktop\perrier\conf;C:\users\cliffc\Desktop\perrier\assembly\target\scala-2.10\spark-assembly-1.1.0-SNAPSHOT-hadoop2.4.0.jar"
#JAVA_OPTS="-XX:MaxPermSize=128m -Djline.terminal=unix -Djava.library.path= -Xms512m -Xmx512m"
#SPARK_ENV_LOADED=1
#SPARK_HOME=/cygdrive/c/users/cliffc/Desktop/perrier
#SPARK_SUBMIT_OPTS= -Djline.terminal=unix
#SPARK_TOOLS_JAR=/cygdrive/c/users/cliffc/Desktop/perrier/tools/target/spark-tools_2.10-1.1.0-SNAPSHOT.jar
#java -cp ".;C:\users\cliffc\Desktop\perrier\conf;C:\users\cliffc\Desktop\perrier\assembly\target\scala-2.10\spark-assembly-1.1.0-SNAPSHOT-hadoop2.4.0.jar" -XX:MaxPermSize=128m -Djline.terminal=unix -Djava.library.path= -Xms512m -Xmx512m org.apache.spark.deploy.SparkSubmit --class org.apache.spark.repl.Main spark-shell
