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





}
