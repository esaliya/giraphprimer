package org.saliya.giraphprimer.multilinear.giraph.unweighted;

import org.apache.giraph.GiraphRunner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.util.ToolRunner;

/**
 * Saliya Ekanayake on 2/2/17.
 */
public class MultilinearMain {

    /*

    org.saliya.giraphprimer.multilinear.giraph.unweighted.MultilinearWorker
-vif
org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat
-vip
src/main/resources/path_graph.txt
-vof
org.apache.giraph.io.formats.IdWithValueTextOutputFormat
-op
src/main/resources/output
-w
1
-ca
mapred.job.tracker=local
-ca
giraph.SplitMasterWorker=false
-ca
giraph.masterComputeClass=org.saliya.giraphprimer.multilinear.giraph.unweighted.MultilinearMaster

     */
    public static final String MULTILINEAR_N = "multilinear.n";
    public static final String MULTILINEAR_K = "multilinear.k";
    public static final String MULTILINEAR_NUM_COLORS = "multilinear.num.colors";
    public static final String MULTILINEAR_SEED = "multilinear.seed";
    public static void main(String[] args) throws Exception {
        int n = Integer.parseInt(args[0]);
        int k = Integer.parseInt(args[1]);
        int numColors = Integer.parseInt(args[2]); // num of colors
        int seed = Integer.parseInt(args[3]); // see for the random instance
        String vertexInputPath = args[4];
        String outputPath = args[5];
        String workers = args[6];
        String jobTrackerURL = args[7];
        String splitMasterWorker = args[8];

        int argIdx = 8;
        String numComputeThreads = (args.length > argIdx+1) ? args[argIdx+1] : "1";
        String maxPartitionsInMemory = (args.length > argIdx+2) ? args[argIdx+2] : "100";
        String Xmx = (args.length > argIdx+3) ? args[argIdx+3] : "4";
        String requestSize = (args.length > argIdx+4) ? args[argIdx+4] : "1024";

        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setInt(MULTILINEAR_N, n);
        conf.setInt(MULTILINEAR_K, k);
        conf.setInt(MULTILINEAR_NUM_COLORS, numColors);
        conf.setLong(MULTILINEAR_SEED, seed);


        GiraphRunner runner = new GiraphRunner();
        runner.setConf(conf);

        System.exit(ToolRunner.run(runner, new String[]{
                MultilinearWorker.class.getName(),
                "-vip", vertexInputPath,
                "-vif", VInputFormat.class.getName(),
//                "-vof", "org.apache.giraph.io.formats.IdWithValueTextOutputFormat",
//                "-op", outputPath,
                "-w", workers,
                "-ca", "mapred.job.tracker="+jobTrackerURL,
                "-ca", "giraph.SplitMasterWorker="+splitMasterWorker,
                "-ca", "giraph.masterComputeClass="+MultilinearMaster.class.getName(),
                "-ca", "giraph.workerContextClass="+MultilinearWorkerContext.class.getName(),
                "-ca", "giraph.useSuperstepCounters=false",
                "-ca", "giraph.messageEncodeAndStoreType=EXTRACT_BYTEARRAY_PER_PARTITION",
                "-ca", "giraph.isStaticGraph=true",
                "-ca", "giraph.useNettyDirectMemory=true",
                "-ca", "giraph.useUnsafeSerialization=true",
                "-ca", "giraph.maxPartitionsInMemory="+maxPartitionsInMemory,
                "-ca", "mapred.child.java.opts=\"-Xmx"+Xmx+"\"G",
                "-ca", "giraph.vertexRequestSize=" + requestSize,
                "-ca", "giraph.edgeRequestSize=" + requestSize,
                "-ca", "giraph.msgRequestSize=" + requestSize,
                "-ca", "giraph.clientSendBufferSize=" + requestSize,
                "-ca", "giraph.checkpointFrequency=0",
                "-ca", "giraph.numComputeThreads="+numComputeThreads}));
    }






}
