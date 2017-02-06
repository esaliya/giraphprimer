#!/bin/bash

# local master
#rm -rf /home/esaliya/sali/projects/vt/giraph/gout.path
#java -cp /home/esaliya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.withmaster.customdata.MultilinearMain 5 3 5 10 /home/esaliya/sali/projects/vt/giraph/path_graph_simple.txt /home/esaliya/sali/projects/vt/giraph/gout.path 1 local false

$HADOOP_HOME/bin/hdfs dfs -rm -r -f /gout.path
$HADOOP_HOME/bin/hadoop jar /home/esaliya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.withmaster.customdata.MultilinearMain 5 3 5 10 /gin.path/path_graph_simple.txt /gout.path 4 hsw003:5431 true
