package org.saliya.giraphprimer.multilinear.giraph.weighted;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.saliya.giraphprimer.multilinear.GaloisField;
import org.saliya.giraphprimer.multilinear.Polynomial;
import org.saliya.giraphprimer.multilinear.Utils;
import org.saliya.giraphprimer.multilinear.giraph.DoubleArrayListWritable;
import org.saliya.giraphprimer.multilinear.giraph.LongArrayWritable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * Saliya Ekanayake on 2/15/17.
 */
public class WeightedMultilinearMaster extends DefaultMasterCompute {
    public static final String W_MULTILINEAR_MAXSUM ="w.multilinear.maxsum";
    public static final String W_MULTILINEAR_WCOLL="w.multilinear.wcoll";
    public static final String W_MULTILINEAR_R="w.multilinear.r";
    public static final String W_MULTILINEAR_RANDOM_NUMS="w.multilinear.random.nums";

    public static GaloisField gf;

    public static int k;
    int n;
    double epsilon,delta,alphaMax;
    long mainSeed;
    String outputFile;
    double roundingFactor;
    public static int r;
    int workerSteps;
    int twoRaisedToK;

    long startTime;

    public WeightedMultilinearMaster() {

    }

    @Override
    public void compute() {
        long ss = getSuperstep();
        if (ss == 0){
            // the entry point
            startTime = System.currentTimeMillis();
            long [] nums = new long[n];
            Random r = new Random();
            IntStream.range(0, n).forEach(x -> nums[x] = r.nextLong());
            broadcast(W_MULTILINEAR_RANDOM_NUMS, new LongArrayWritable(nums));
            return;
        } else if (ss == 1){
            // aggregate updated vertex weights to compute maxWeight
            ArrayList<Double> weights = this.<DoubleArrayListWritable>getAggregatedValue(W_MULTILINEAR_WCOLL).getData();
            Collections.sort(weights);
            double maxWeight = 0;
            for (int i = weights.size() - 1; i >= weights.size() - k; i--) {
                maxWeight += weights.get(i);
            }
            System.out.println("*** Max Weight: " + maxWeight);
            r = (int) Math.ceil(Utils.logb((int) maxWeight + 1, roundingFactor));
            // invalid input: r is negative
            if (r < 0) {
                throw new IllegalArgumentException("r must be a positive integer or 0");
            }
            broadcast(W_MULTILINEAR_R, new IntWritable(r));
            return;
        }

        long effectiveSS = ss - 1;
        // localSS == 0 means it's a new loop of the 2^k loops
        int localSS = (int)effectiveSS * workerSteps;
        // The external loop number that goes from 0 to twoRaisedToK (excluding)
        int iter = (int)effectiveSS / workerSteps;

        if (effectiveSS > 0 && localSS == 0){
            // this indicates previous 2^k loop has finished,
            // so anything from that step that needs to get
            // aggregated can be done here, but for this program
            // no need of aggregated value for each 2^k
        }

        if (iter == twoRaisedToK){
            // TODO: do final computation of results
            double bestScore = this.<DoubleWritable>getAggregatedValue(W_MULTILINEAR_MAXSUM).get();
            long duration = System.currentTimeMillis() - startTime;
            System.out.println("*** End of program bestScore for this giraph run: " + bestScore + " time: " +
                    duration +  " ms");
            haltComputation();
        }

    }

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        Configuration conf = getConf();
        n = conf.getInt(WeightedMultilinearMain.W_MULTILINEAR_N, -1);
        k = conf.getInt(WeightedMultilinearMain.W_MULTILINEAR_K, -1);
        delta = conf.getDouble(WeightedMultilinearMain.W_MULTILINEAR_DELTA, -1.0);
        alphaMax = conf.getDouble(WeightedMultilinearMain.W_MULTILINEAR_ALPHAMAX, -1.0);
        mainSeed = conf.getLong(WeightedMultilinearMain.W_MULTILINEAR_MAIN_SEED, -1);
        outputFile = conf.get(WeightedMultilinearMain.W_MULTILINEAR_OUTPUT);

        roundingFactor = 1+delta;
        registerAggregator(W_MULTILINEAR_WCOLL, DoubleCollector.class);

        long mainSeed = conf.getLong(WeightedMultilinearMain.W_MULTILINEAR_MAIN_SEED, -1);
        Random r = new Random(mainSeed);
        // (1 << k) is 2 raised to the kth power
        twoRaisedToK = (1 << k);
        int degree = (3 + Utils.log2(k));
        gf = GaloisField.getInstance(1 << degree, Polynomial.createIrreducible(degree, r).toBigInteger().intValue());
        int maxIterations = k-1; // the original pregel loop was from 2 to k (including k), so that's (k-2)+1 times
        workerSteps = maxIterations+1; // the first worker step is used to initialize, so need k iterations
        registerAggregator(W_MULTILINEAR_MAXSUM, DoubleMaxAggregator.class);
    }

    public static class DoubleCollector extends BasicAggregator<DoubleArrayListWritable>{
        @Override
        public void aggregate(DoubleArrayListWritable value) {
            getAggregatedValue().getData().addAll(value.getData());
        }

        @Override
        public DoubleArrayListWritable createInitialValue() {
            return new DoubleArrayListWritable();
        }
    }

//    public static class IntMatrixAggregator extends BasicAggregator<IntMatrixWritable>{
//        @Override
//        public void aggregate(IntMatrixWritable value) {
//            int[][] runningAggregatedValue = getAggregatedValue().getData();
//            int[][] incomingValue = value.getData();
//            for (int kPrime = 0; kPrime <= k; kPrime++) {
//                for (int rPrime = 0; rPrime <= r; rPrime++) {
//                    runningAggregatedValue[kPrime][rPrime] = gf.add(runningAggregatedValue[kPrime][rPrime],
//                            incomingValue[kPrime][rPrime]);
//                }
//            }
//        }
//
//        @Override
//        public IntMatrixWritable createInitialValue() {
//            int[][] matrix = new int[k+1][r+1];
//            for (int i = 0; i <= k; i++) {
//                for (int j = 0; j <= r; j++) {
//                    matrix[i][j] = 0;
//                }
//            }
//            return new IntMatrixWritable(matrix);
//        }
//    }
}
