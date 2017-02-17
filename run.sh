#!/bin/bash

# local master
#rm -rf /N/u/sekanaya/sali/projects/vt/giraph/gout.path
#java -cp /N/u/sekanaya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.withmaster.customdata.MultilinearMain 5 3 5 10 /N/u/sekanaya/sali/projects/vt/giraph/path_graph_simple.txt /N/u/sekanaya/sali/projects/vt/giraph/gout.path 1 local false

$HADOOP_HOME/bin/hdfs dfs -rm -r -f /gout.path
#path graph 1
#$HADOOP_HOME/bin/hadoop jar /N/u/sekanaya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.withmaster.customdata.MultilinearMain 5 3 5 10 /gin.path/path_graph_simple.txt /gout.path 4 j-001:5431 true

#path graph 2
#$HADOOP_HOME/bin/hadoop jar /N/u/sekanaya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.withmaster.customdata.MultilinearMain 5 3 3 10 /gin.path.2/path_graph_simple_2.txt /gout.path 4 j-001:5431 true

#syn.g.100
#$HADOOP_HOME/bin/hadoop jar /N/u/sekanaya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.withmaster.customdata.MultilinearMain 100 8 7 10 /syn.g.100/er-k8-0-n100-p20.txt /gout.path 4 j-001:5431 true

#syn.g.5k
#$HADOOP_HOME/bin/hadoop jar /N/u/sekanaya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.withmaster.customdata.MultilinearMain 5000 8 7 10 /syn.g.5k/er-k8-0-n5000-p20.txt /gout.path 12 j-001:5431 true 2 1000

#snap.v10k.e40k
#$HADOOP_HOME/bin/hadoop jar /N/u/sekanaya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.withmaster.customdata.MultilinearMain 10876 8 7 10 /snap.v10k.e40k/p2p-Gnutella04_simple_k8.txt /gout.path 12 j-001:5431 true 2 1000

#snap.v36k.e88k
#$HADOOP_HOME/bin/hadoop jar /N/u/sekanaya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.withmaster.customdata.MultilinearMain 36682 8 7 10 /snap.v36k.e88k/p2p-Gnutella30_simple_k8.txt /gout.path 12 j-001:5431 true 2 1000 20

#snap.v4.8m.e68m
$HADOOP_HOME/bin/hadoop jar /N/u/sekanaya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.withmaster.customdata.MultilinearMain 4847571 4 7 10 /snap.v4.8m.e68m/soc-LiveJournal1_simple_k8.txt /gout.path 192 j-001:5431 true 2 1000 5

#$HADOOP_HOME/bin/hdfs dfs -rm -r -f /gout.path
#$HADOOP_HOME/bin/hadoop jar /N/u/sekanaya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.withmaster.customdata.MultilinearMain 4847571 5 7 10 /snap.v4.8m.e68m/soc-LiveJournal1_simple_k8.txt /gout.path 192 j-001:5431 true 2 1000 5

#$HADOOP_HOME/bin/hdfs dfs -rm -r -f /gout.path
#$HADOOP_HOME/bin/hadoop jar /N/u/sekanaya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.withmaster.customdata.MultilinearMain 4847571 6 7 10 /snap.v4.8m.e68m/soc-LiveJournal1_simple_k8.txt /gout.path 192 j-001:5431 true 2 1000 5

#$HADOOP_HOME/bin/hdfs dfs -rm -r -f /gout.path
#$HADOOP_HOME/bin/hadoop jar /N/u/sekanaya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.withmaster.customdata.MultilinearMain 4847571 7 7 10 /snap.v4.8m.e68m/soc-LiveJournal1_simple_k8.txt /gout.path 192 j-001:5431 true 2 1000 5

#$HADOOP_HOME/bin/hdfs dfs -rm -r -f /gout.path
#$HADOOP_HOME/bin/hadoop jar /N/u/sekanaya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.withmaster.customdata.MultilinearMain 4847571 2 7 10 /snap.v4.8m.e68m/soc-LiveJournal1_simple_k8.txt /gout.path 192 j-001:5431 true 2 1000 5

#syn.g.1k
#$HADOOP_HOME/bin/hadoop jar /N/u/sekanaya/sali/git/github/esaliya/java/giraphprimer/target/giraphprimer-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.giraphprimer.withmaster.customdata.MultilinearMain 1000 8 7 10 /syn.g.1k/er-k8-0-n1000-p20.txt /gout.path 12 j-001:5431 true 2 1000
