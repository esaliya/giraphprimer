package org.saliya.giraphprimer.withmaster.customdata;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.saliya.giraphprimer.multilinear.GaloisField;
import org.saliya.giraphprimer.multilinear.Polynomial;
import org.saliya.giraphprimer.multilinear.Utils;

import java.util.Random;

/**
 * Saliya Ekanayake on 2/3/17.
 */
public class MultilinearWorkerContext extends WorkerContext{

    static int workerSteps;
    static int twoRaisedToK;
    static int fieldSize;
    static GaloisField gf = null;

    int n;
    int k;
    int numColors;
    long randomSeed;
    int [] randomAssignment;

    @Override
    public void preApplication() throws InstantiationException, IllegalAccessException {
        System.out.println("### Call to preApplication on" + getMyWorkerIndex());
        Configuration conf = getConf();
        n = conf.getInt(MultilinearMain.MULTILINEAR_N, -1);
        k = conf.getInt(MultilinearMain.MULTILINEAR_K, -1);
        numColors = conf.getInt(MultilinearMain.MULTILINEAR_NUM_COLORS, -1);
        long seed = conf.getLong(MultilinearMain.MULTILINEAR_SEED, -1);


        int degree = 3+Utils.log2(k);
        int maxIterations = k-1; // the original pregel loop was from 2 to k (including k), so that's (k-2)+1 times
        workerSteps = maxIterations+1; // the first worker step is used to initialize, so need k iterations
        Random r = new Random(seed);
        twoRaisedToK = 1 << k;
        gf = GaloisField.getInstance(1 << degree, Polynomial.createIrreducible(degree, r).toBigInteger().intValue());
        fieldSize = gf.getFieldSize();
        randomAssignment = new int[numColors];
        for (int i = 0; i < numColors; ++i){
            randomAssignment[i] = r.nextInt(twoRaisedToK);
        }
        randomSeed = r.nextLong();
    }

    @Override
    public void postApplication() {

    }

    @Override
    public void preSuperstep() {

    }

    @Override
    public void postSuperstep() {

    }

    public static class MultilinearMaster extends DefaultMasterCompute{
        public static final String MULTILINEAR_CIRCUIT_SUM="multilinear.circuitsum";

        @Override
        public void compute() {
            long ss = getSuperstep();

            // nothing to do on superstep zero on master compute
            // this is done because preApplication() in WorkerContext runs
            // only after master compute() but before worker compute()
            // so to make sure initialization work in preApplication() has happened
            // before any of those are used we let got of superstep zero on master.
            if (ss == 0) return;

            if (workerSteps == 0){
                System.out.println("*************Error: ss=" + ss);

            }

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
            registerAggregator(MULTILINEAR_CIRCUIT_SUM, GaloisFieldAggregator.class);
        }
    }

    public static class GaloisFieldAggregator extends BasicAggregator<IntWritable>{

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
