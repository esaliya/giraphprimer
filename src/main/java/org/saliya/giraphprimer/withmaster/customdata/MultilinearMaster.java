package org.saliya.giraphprimer.withmaster.customdata;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.saliya.giraphprimer.multilinear.GaloisField;
import org.saliya.giraphprimer.multilinear.Polynomial;
import org.saliya.giraphprimer.multilinear.Utils;

import java.util.Random;

/**
 * Saliya Ekanayake on 2/6/17.
 */
public class MultilinearMaster extends DefaultMasterCompute {
    public static final String MULTILINEAR_CIRCUIT_SUM="multilinear.circuitsum";

    // Have to define these here as well because a worker context may
    // not be available where the master vertex runs
    public static GaloisField gf = null;
    int twoRaisedToK;
    int workerSteps;
    @Override
    public void compute() {
        long ss = getSuperstep();

        // nothing to do on superstep zero on master compute
        if (ss == 0) return;

        int localSS = (int)ss % workerSteps;
        // The external loop number that goes from 0 to twoRaisedToK (excluding)
        int iter = (int)ss / workerSteps;

        int totalSum = 0;
        if (ss > 0 && localSS == 0){
            // get the aggregated value from previous loop (of 2^k loops)
            totalSum = gf.add(totalSum,
                    this.<IntWritable>getAggregatedValue(MULTILINEAR_CIRCUIT_SUM).get());
        }

        if (iter == twoRaisedToK){
            // End of computation and application
            boolean answer = totalSum > 0;
            System.out.println("*** End of program returned " + answer);
            haltComputation();
        }

    }

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        Configuration conf = getConf();
        int k = conf.getInt(MultilinearMain.MULTILINEAR_K, -1);
        int degree = 3+ Utils.log2(k);
        twoRaisedToK = 1 << k;
        int maxIterations = k-1; // the original pregel loop was from 2 to k (including k), so that's (k-2)+1 times
        workerSteps = maxIterations+1; // the first worker step is used to initialize, so need k iterations

        long seed = conf.getLong(MultilinearMain.MULTILINEAR_SEED, -1);
        Random r = new Random(seed);
        gf = GaloisField.getInstance(1 << degree, Polynomial.createIrreducible(degree, r).toBigInteger().intValue());
        registerAggregator(MULTILINEAR_CIRCUIT_SUM, GaloisFieldAggregator.class);
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
            if (gf == null) return 0;
            return gf.add(x,y);
        }
    }
}
