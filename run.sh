#!/bin/bash

# local master
#rm -rf /home/esaliya/sali/projects/vt/giraph/gout.path
#java -cp /home/esaliya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.multilinear.giraph.unweighted.MultilinearMain 5 3 5 10 /home/esaliya/sali/projects/vt/giraph/path_graph_simple.txt /home/esaliya/sali/projects/vt/giraph/gout.path 1 local false

#$HADOOP_HOME/bin/hdfs dfs -rm -r -f /gout.path
#path graph 1
#$HADOOP_HOME/bin/hadoop jar /home/esaliya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.multilinear.giraph.unweighted.MultilinearMain 5 3 5 10 /gin.path/path_graph_simple.txt /gout.path 4 hsw003:5431 true

$HADOOP_HOME/bin/hdfs dfs -rm -r -f /gout.path
#path graph 2
#$HADOOP_HOME/bin/hadoop jar /home/esaliya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.multilinear.giraph.unweighted.MultilinearMain 5 3 3 10 /gin.path.2/path_graph_simple_2.txt /gout.path 4 hsw003:5431 true

#syn.g.1
#$HADOOP_HOME/bin/hadoop jar /home/esaliya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.multilinear.giraph.unweighted.MultilinearMain 100 8 7 10 /syn.g.1/er-k8-0-n100-p20.txt /gout.path 4 hsw003:5431 true
#syn.g.5k
#$HADOOP_HOME/bin/hadoop jar /home/esaliya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.multilinear.giraph.unweighted.MultilinearMain 5000 8 7 10 /syn.g.5k/er-k8-0-n5000-p20.txt /gout.path 16 hsw003:5431 true
#syn.g.1k
$HADOOP_HOME/bin/hadoop jar /home/esaliya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.withmaster.customdata.MultilinearMain 1000 8 7 10 /syn.g.1k/er-k8-0-n1000-p20.txt /gout.path 100 hsw003:5431 true
