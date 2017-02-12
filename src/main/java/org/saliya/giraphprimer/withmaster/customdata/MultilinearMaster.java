package org.saliya.giraphprimer.withmaster.customdata;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.saliya.giraphprimer.LongArrayWritable;
import org.saliya.giraphprimer.multilinear.GaloisField;
import org.saliya.giraphprimer.multilinear.Polynomial;
import org.saliya.giraphprimer.multilinear.Utils;

import java.util.Random;
import java.util.stream.IntStream;

/**
 * Saliya Ekanayake on 2/6/17.
 */
public class MultilinearMaster extends DefaultMasterCompute {
    public static final String MULTILINEAR_CIRCUIT_SUM="multilinear.circuitsum";
    public static final String MULTILINEAR_RANDOM_NUMS="multilinear.random.nums";
    public static final String MULTILINEAR_COMPUTE_TIME="multilinear.compute.time";
    public static final String MULTILINEAR_SORT_TIME="multilinear.sort.time";

    // Have to define these here as well because a worker context may
    // not be available where the master vertex runs
    public static GaloisField gf = null;
    int twoRaisedToK;
    int workerSteps;
    int n;

    long startTime;

    private long aggregateTime = 0L;

    @Override
    public void compute() {
        long ss = getSuperstep();

        // nothing to do on superstep zero on master compute
        if (ss == 0) {
            startTime = System.currentTimeMillis();
            // Generate a random number array
            long [] nums = new long[n];
            Random random = new Random();
            IntStream.range(0, n).forEach(x -> nums[x] = random.nextLong());
            broadcast(MULTILINEAR_RANDOM_NUMS, new LongArrayWritable(nums));
            return;
        }

        int localSS = (int)ss % workerSteps;
        // The external loop number that goes from 0 to twoRaisedToK (excluding)
        int iter = (int)ss / workerSteps;

        int totalSum = 0;
        if (ss > 0 && localSS == 0){
            long t = System.currentTimeMillis();
            // get the aggregated value from previous loop (of 2^k loops)
            int aggregatedValue = this.<IntWritable>getAggregatedValue(MULTILINEAR_CIRCUIT_SUM).get();
            aggregateTime += System.currentTimeMillis() - t;
            //System.out.println("DEBUG: aggregated value: " + aggregatedValue + " iter:  " + iter);
            totalSum = gf.add(totalSum, aggregatedValue);
        }

        if (iter == twoRaisedToK){
            // End of computation and application
            boolean answer = totalSum > 0;
            long duration = System.currentTimeMillis() - startTime;
            long totalComputeTime = this.<LongWritable>getAggregatedValue(MULTILINEAR_COMPUTE_TIME).get();
            long totalSortTime = this.<LongWritable>getAggregatedValue(MULTILINEAR_SORT_TIME).get();
            long totalSortPlusComputeTime = totalComputeTime + totalSortTime;

//            System.out.println("*** End of program returned " + answer + " in " + duration + " ms  total supersteps " + getSuperstep() + " iter: " + iter + " localSS: " + localSS + " workerSteps: " + workerSteps + " k:" + getConf().getInt(MultilinearMain.MULTILINEAR_K, -1));
            System.out.println("*** End of program returned " + answer + " in " + duration + " ms avgComputeTime(perVertex): " + (totalComputeTime*1.0/n) + " ms avgSortTime(perVertex): " + (totalSortTime*1.0/n) + " ms avgSortPlusComputePercent(perVertex) " + (totalSortPlusComputeTime*100.0/(n*duration)) + " aggregatePercent " + (aggregateTime*100.0/duration));
            haltComputation();
        }

    }

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        Configuration conf = getConf();
        n = conf.getInt(MultilinearMain.MULTILINEAR_N, -1);
        int k = conf.getInt(MultilinearMain.MULTILINEAR_K, -1);
        int degree = 3+ Utils.log2(k);
        twoRaisedToK = 1 << k;
        int maxIterations = k-1; // the original pregel loop was from 2 to k (including k), so that's (k-2)+1 times
        workerSteps = maxIterations+1; // the first worker step is used to initialize, so need k iterations

        long seed = conf.getLong(MultilinearMain.MULTILINEAR_SEED, -1);
        Random r = new Random(seed);
        gf = GaloisField.getInstance(1 << degree, Polynomial.createIrreducible(degree, r).toBigInteger().intValue());
        registerAggregator(MULTILINEAR_CIRCUIT_SUM, GaloisFieldAggregator.class);
        registerAggregator(MULTILINEAR_COMPUTE_TIME,  LongSumAggregator.class);
        registerAggregator(MULTILINEAR_SORT_TIME, LongSumAggregator.class);
    }

    public static class GaloisFieldAggregator extends BasicAggregator<IntWritable> {

        @Override
        public void aggregate(IntWritable value) {
            getAggregatedValue().set(add(getAggregatedValue().get(), value.get()));
        }

        @Override
        public IntWritable createInitialValue() {
            return new IntWritable(0);
        }


        int add(int x, int y) {
            // this was necessary because the first call to aggregate comes before
            // preApplication in WorkerContext, where gf is null
//            if (gf == null) {
//                System.out.println("GF is null in aggregator");
//                return 0;
//            }
//            return gf.add(x,y);

            return x^y;
        }
    }
}
