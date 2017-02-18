#!/bin/bash

# local master

$HADOOP_HOME/bin/hdfs dfs -rm -r -f /gout.path

#snap.v4.8m.e68m
#$HADOOP_HOME/bin/hadoop jar /N/u/sekanaya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.withmaster.customdata.MultilinearMain 4847571 4 7 10 /snap.v4.8m.e68m/soc-LiveJournal1_simple_k8.txt /gout.path 192 j-001:5431 true 2 1000 5

#random-er/1k
#$HADOOP_HOME/bin/hadoop jar /N/u/sekanaya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.multilinear.giraph.weighted.WeightedMultilinearMain run-non-param-power 1000 4 10 0.1 1 /random-er/1k/0001000.txt /gout.path 12 j-001:5431 true 2 1000 5

#random-er/10k
#$HADOOP_HOME/bin/hadoop jar /N/u/sekanaya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.multilinear.giraph.weighted.WeightedMultilinearMain run-non-param-power 10000 4 10 0.1 1 /random-er/10k/0010000.txt /gout.path 12 j-001:5431 true 2 1000 5

#random-er/10k
#$HADOOP_HOME/bin/hadoop jar /N/u/sekanaya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.multilinear.giraph.weighted.WeightedMultilinearMain run-non-param-power 1000000 4 10 0.1 1 /random-er/1mil/1000000.txt /gout.path 12 j-001:5431 true 2 1000 5

#random-er/10k
#$HADOOP_HOME/bin/hadoop jar /N/u/sekanaya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.multilinear.giraph.weighted.WeightedMultilinearMain run-non-param-power 100 4 10 0.1 1 /random-er/100/0000100.txt /gout.path 12 j-001:5431 true 2 1000 5

#lj/soc-LiveJournal1-refined.txt
$HADOOP_HOME/bin/hadoop jar /N/u/sekanaya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.multilinear.giraph.weighted.WeightedMultilinearMain run-non-param-power 48438464 1 10 0.1 1 /lj/soc-LiveJournal1-refined.txt /gout.path 192 j-001:5431 true 2 1000 5
