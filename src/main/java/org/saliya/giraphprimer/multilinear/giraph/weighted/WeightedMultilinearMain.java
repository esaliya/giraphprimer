package org.saliya.giraphprimer.multilinear.giraph.weighted;

import org.apache.giraph.GiraphRunner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.util.ToolRunner;

import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Saliya Ekanayake on 2/15/17.
 */
public class WeightedMultilinearMain {
    public static final String W_MULTILINEAR_N = "w.multilinear.n";
    public static final String W_MULTILINEAR_K = "w.multilinear.k";
    public static final String W_MULTILINEAR_DELTA = "w.multilinear.delta";
    public static final String W_MULTILINEAR_ALPHAMAX = "w.multilinear.alphamax";
    public static final String W_MULTILINEAR_MAIN_SEED = "w.multilinear.main.seed";
    public static final String W_MULTILINEAR_OUTPUT = "w.multilinear.output";

    static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
    public static void main(String[] args) throws Exception {
        String option = args[0];
        if (!"run-non-param-power".equals(option)) {
            System.out.println("ERROR: type " + option + " is not supported!");
            return;
        }
        int n = Integer.parseInt(args[1]);
        int k = Integer.parseInt(args[2]);
        // invalid input: k is non-positive
        if (k <= 0) {
            throw new IllegalArgumentException("k must be a positive integer");
        }
        int seed = Integer.parseInt(args[3]);
        double epsilon = args.length > 4 ? Double.parseDouble(args[4]) : 1;
        double delta = args.length > 5 ? Double.parseDouble(args[5]) : 1;
        double alphaMax = 0.15;

        String vertexInputPath = args[6];
        String outputPath = args[7];
        String workers = args[8];
        String jobTrackerURL = args[9];
        String splitMasterWorker = args[10];

        int argIdx = 10;
        String numComputeThreads = (args.length > argIdx + 1) ? args[argIdx + 1] : "1";
        String maxPartitionsInMemory = (args.length > argIdx + 2) ? args[argIdx + 2] : "100";
        String Xmx = (args.length > argIdx + 3) ? args[argIdx + 3] : "4";
        String requestSize = (args.length > argIdx + 4) ? args[argIdx + 4] : "1024";



        // get number of iterations for a target error bound (epsilon)
        double probSuccess = 0.2;
        int iter = (int) Math.round(Math.log(epsilon) / Math.log(1 - probSuccess));
        iter = Math.max(iter, 1); // run for at least one iteration
        System.out.println(iter + " assignments will be evaluated for epsilon = " + epsilon);
        double roundingFactor = 1 + delta;
        System.out.println("approximation factor is " + roundingFactor);


        // TODO: Change this serial loop to submit parallely
        String date = dateFormat.format(new Date());
        // TODO: change loop to go from 0 to iter (now testing with 1 iter)
//        for (int i = 0; i < iter; i++) {
        for (int i = 0; i < 1; i++) {
            String output = Paths.get(outputPath, date+"_iter_" + i + ".txt").toString();
            GiraphConfiguration conf = new GiraphConfiguration();
            conf.setInt(W_MULTILINEAR_N, n);
            conf.setInt(W_MULTILINEAR_K, k);
            conf.setDouble(W_MULTILINEAR_DELTA, delta);
            conf.setDouble(W_MULTILINEAR_ALPHAMAX, alphaMax);
//            conf.setLong(W_MULTILINEAR_MAIN_SEED, seed);
            conf.setLong(W_MULTILINEAR_MAIN_SEED, System.currentTimeMillis());
            conf.set(W_MULTILINEAR_OUTPUT, output);

            GiraphRunner runner = new GiraphRunner();
            runner.setConf(conf);
            ToolRunner.run(runner, new String[]{
                    WeightedMultilinearWorker.class.getName(),
                    "-vip", vertexInputPath,
                    "-vif", WeightedVInputFormat.class.getName(),
                    "-w", workers,
                    "-ca", "mapred.job.tracker=" + jobTrackerURL,
                    "-ca", "giraph.SplitMasterWorker=" + splitMasterWorker,
                    "-ca", "giraph.masterComputeClass=" + WeightedMultilinearMaster.class.getName(),
                    "-ca", "giraph.workerContextClass=" + WeightedMultilinearWorkerContext.class.getName(),
                    "-ca", "giraph.useSuperstepCounters=false",
                    "-ca", "giraph.messageEncodeAndStoreType=EXTRACT_BYTEARRAY_PER_PARTITION",
                    "-ca", "giraph.isStaticGraph=true",
                    "-ca", "giraph.useNettyDirectMemory=true",
                    "-ca", "giraph.useUnsafeSerialization=true",
                    "-ca", "giraph.maxPartitionsInMemory=" + maxPartitionsInMemory,
                    "-ca", "mapred.child.java.opts=\"-Xmx" + Xmx + "\"G",
                    "-ca", "giraph.vertexRequestSize=" + requestSize,
                    "-ca", "giraph.edgeRequestSize=" + requestSize,
                    "-ca", "giraph.msgRequestSize=" + requestSize,
                    "-ca", "giraph.clientSendBufferSize=" + requestSize,
                    "-ca", "giraph.checkpointFrequency=0",
                    "-ca", "giraph.numComputeThreads=" + numComputeThreads});
        }

        // TODO: combining results from each Giraph run
    }
}
