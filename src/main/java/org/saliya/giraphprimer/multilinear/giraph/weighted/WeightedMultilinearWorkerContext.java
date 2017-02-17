package org.saliya.giraphprimer.multilinear.giraph.weighted;

import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.saliya.giraphprimer.multilinear.GaloisField;
import org.saliya.giraphprimer.multilinear.Polynomial;
import org.saliya.giraphprimer.multilinear.Utils;

import java.util.Random;
import java.util.stream.IntStream;

/**
 * Saliya Ekanayake on 2/15/17.
 */
public class WeightedMultilinearWorkerContext extends WorkerContext{
    int n,k;
    double delta,alphaMax;
    long mainSeed;

    GaloisField gf;
    int twoRaisedToK;
    int[] randomAssignments;
    int[] completionVariables;
    int workerSteps;

    @Override
    public void preApplication() throws InstantiationException, IllegalAccessException {
        Configuration conf = getConf();
        n = conf.getInt(WeightedMultilinearMain.W_MULTILINEAR_N, -1);
        k = conf.getInt(WeightedMultilinearMain.W_MULTILINEAR_K, -1);
        delta = conf.getDouble(WeightedMultilinearMain.W_MULTILINEAR_DELTA, -1.0);
        alphaMax = conf.getDouble(WeightedMultilinearMain.W_MULTILINEAR_ALPHAMAX, -1.0);
        mainSeed = conf.getLong(WeightedMultilinearMain.W_MULTILINEAR_MAIN_SEED, -1);

        long mainSeed = conf.getLong(WeightedMultilinearMain.W_MULTILINEAR_MAIN_SEED, -1);
        Random r = new Random(mainSeed);
        // (1 << k) is 2 raised to the kth power
        twoRaisedToK = (1 << k);
        int degree = (3 + Utils.log2(k));
        gf = GaloisField.getInstance(1 << degree, Polynomial.createIrreducible(degree, r).toBigInteger().intValue());
        randomAssignments = new int[n];
        IntStream.range(0,n).forEach(i->randomAssignments[i] = r.nextInt(twoRaisedToK));
        completionVariables = new int[k-1];
        IntStream.range(0,k-1).forEach(i->completionVariables[i] = r.nextInt(twoRaisedToK));
        int maxIterations = k-1; // the original pregel loop was from 2 to k (including k), so that's (k-2)+1 times
        workerSteps = maxIterations+1; // the first worker step is used to initialize, so need k iterations
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
