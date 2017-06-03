package org.saliya.giraphprimer.multilinear.giraph.weighted;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.saliya.giraphprimer.multilinear.Utils;
import org.saliya.giraphprimer.multilinear.giraph.DoubleArrayListWritable;
import org.saliya.giraphprimer.multilinear.giraph.IntMatrixWritable;
import org.saliya.giraphprimer.multilinear.giraph.LongArrayWritable;
import org.saliya.giraphprimer.multilinear.giraph.ShortArrayWritable;

import java.io.IOException;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

/**
 * Saliya Ekanayake on 2/15/17.
 */
public class WeightedMultilinearWorker extends BasicComputation<
        IntWritable, WeightedVData, NullWritable, IntMatrixWritable> {

    // Note: Unless variables are set at preSuperstep()
    // they may not get preserved
    private WeightedMultilinearWorkerContext wmwc;
    private double roundingFactor;

    private int k;

    @Override
    public void compute(
            Vertex<IntWritable, WeightedVData, NullWritable> vertex,
            Iterable<IntMatrixWritable> messages) throws IOException {

        WeightedVData weightedVData = vertex.getValue();
        long t = System.currentTimeMillis();

        long ss = getSuperstep();
        if (ss == 0){

            // aggregate original value (this is used to find max weight) before the update
            aggregate(WeightedMultilinearMaster.W_MULTILINEAR_WCOLL,
                    new DoubleArrayListWritable(weightedVData.vertexWeight));
            weightedVData.vertexWeight
                    = (int) Math.ceil(
                            Utils.logb((int) weightedVData.vertexWeight + 1,
                                    roundingFactor));
            long[] nums = this.<LongArrayWritable>getBroadcast(WeightedMultilinearMaster.W_MULTILINEAR_RANDOM_NUMS).getData();
            weightedVData.uniqueRandomSeed = nums[weightedVData.vertexId];
            weightedVData.compDuration += System.currentTimeMillis() - t;
            return;
        } else if (ss == 1){
            weightedVData.r = this.<IntWritable>getBroadcast(WeightedMultilinearMaster.W_MULTILINEAR_R).get();
        }

        // ss=0 was used for update node weights
        // so the effectiveSS is what matters
        // for evaluateCircuit.
        long effectiveSS = ss - 1;
        if (effectiveSS == 0){
            // Create totalSumTable and initialize it once for all of the 2^k iterations
            weightedVData.totalSumTable = new int[k+1][weightedVData.r+1];
            for (int i = 0; i <= k; i++) {
                for (int j = 0; j <= weightedVData.r; j++) {
                    weightedVData.totalSumTable[i][j] = 0;
                }
            }
        }

        // localSS == 0 means it's a new loop of the 2^k loops
        int localSS = (int)effectiveSS % wmwc.workerSteps;
        // The external loop number that goes from 0 to twoRaisedToK (excluding)
        int iter = (int)effectiveSS / wmwc.workerSteps;
        // In the original code I started from 2 and went till k (including)
        // we start localSS from zero, which is for initialization
        // then real work begins with localSS=1, which should give I=2, hence I=localSS+1
        int I = localSS+1;
        if (localSS == 0){
            // the ith index of this array contains the polynomial
            // y_1 * y_2 * ... y_i
            // where y_0 is defined as the identity of the finite field, (i.e., 1)
            weightedVData.cumulativeCompletionVariables = new int[k];
            weightedVData.cumulativeCompletionVariables[0] = 1;
            for (int i = 1; i < k; i++) {
                int dotProduct = (wmwc.completionVariables[i - 1] & iter); // dot product is bitwise and
                weightedVData.cumulativeCompletionVariables[i] = (Integer.bitCount(dotProduct) % 2 == 1) ? 0 :
                        weightedVData.cumulativeCompletionVariables[i - 1];
            }

            // create the vertex unique random variable
            weightedVData.random = new Random(weightedVData.uniqueRandomSeed);
            // set arrays in vertex data
            weightedVData.optTable = new int[k+1][weightedVData.r+1];
            weightedVData.extTable = new int[k+1][weightedVData.r+1];
            int nodeWeight = (int)weightedVData.vertexWeight;
            int dotProduct = (wmwc.randomAssignments[weightedVData.vertexId] & iter); // dot product is bitwise and
            int eigenvalue = (Integer.bitCount(dotProduct) % 2 == 1) ? 0 : 1;
            for (int i = 0; i <= weightedVData.r; i++) {
                weightedVData.optTable[1][i] = weightedVData.extTable[1][i] = 0;
            }
            weightedVData.optTable[1][nodeWeight] = eigenvalue;
            weightedVData.extTable[1][nodeWeight] = eigenvalue * weightedVData.cumulativeCompletionVariables[k - 1];
        } else {
            // business logic only if localSS != 0

            TreeMap<Integer, IntMatrixWritable> messagesSortedByVertexId = new TreeMap<>();
            for (IntMatrixWritable message: messages){
                messagesSortedByVertexId.put(message.getVertexId(), message);
            }

            Set<Integer> neighborsInAscendingOrder = messagesSortedByVertexId.keySet();

            int fieldSize = wmwc.gf.getFieldSize();
            int[][] optTable = weightedVData.optTable;
            Random random = weightedVData.random;
            // for every quota l from 0 to r
            for (int l = 0; l <= weightedVData.r; l++) {
                // initialize the polynomial P_{i,u,l}
                int polynomial = 0;
                // recursive step:
                // iterate through all the pairs of polynomials whose sizes add up to i
                for (int iPrime = 1; iPrime < I; iPrime++) {
                    for (int v: neighborsInAscendingOrder) {
                        int[][] neighborOptTable = messagesSortedByVertexId.get(v).getData();
                        if (l == 0) {
                            int weight = random.nextInt(fieldSize);
                            int product = wmwc.gf.multiply(optTable[iPrime][0], neighborOptTable[I - iPrime][0]);
                            product = wmwc.gf.multiply(weight, product);
                            polynomial = wmwc.gf.add(polynomial, product);
                        } else if (l == 1) {
                            int weight = random.nextInt(fieldSize);
                            int product = wmwc.gf.multiply(optTable[iPrime][1], neighborOptTable[I - iPrime][0]);
                            product = wmwc.gf.multiply(weight, product);
                            polynomial = wmwc.gf.add(polynomial, product);

                            weight = random.nextInt(fieldSize);
                            product = wmwc.gf.multiply(optTable[iPrime][0], neighborOptTable[I - iPrime][1]);
                            product = wmwc.gf.multiply(weight, product);
                            polynomial = wmwc.gf.add(polynomial, product);
                        } else {
                            int weight = random.nextInt(fieldSize);
                            int product = wmwc.gf.multiply(optTable[iPrime][l - 1], neighborOptTable[I - iPrime][l -
                                    1]);
                            product = wmwc.gf.multiply(weight, product);
                            polynomial = wmwc.gf.add(polynomial, product);

                            weight = random.nextInt(fieldSize);
                            product = wmwc.gf.multiply(optTable[iPrime][l], neighborOptTable[I - iPrime][0]);
                            product = wmwc.gf.multiply(weight, product);
                            polynomial = wmwc.gf.add(polynomial, product);

                            weight = random.nextInt(fieldSize);
                            product = wmwc.gf.multiply(optTable[iPrime][0], neighborOptTable[I - iPrime][l]);
                            product = wmwc.gf.multiply(weight, product);
                            polynomial = wmwc.gf.add(polynomial, product);
                        }
                    }
                }
                optTable[I][l] = polynomial;
                if (weightedVData.cumulativeCompletionVariables[k - I] != 0) {
                    weightedVData.extTable[I][l] = optTable[I][l];
                }
            }

        }


        if (localSS != (wmwc.workerSteps -1)){
            sendMessageToAllEdges(vertex, new IntMatrixWritable(weightedVData.optTable, weightedVData.vertexId));
        } else {
            // aggregate to master
            // hmm, but we don't need to aggregate, just add to totalSumTable of the vertex
            int[][] tableTotalSum = weightedVData.totalSumTable;
            int[][] tableForNode = weightedVData.extTable;
            for (int kPrime = 0; kPrime <= k; kPrime++) {
                for (int rPrime = 0; rPrime <= weightedVData.r; rPrime++) {
                    tableTotalSum[kPrime][rPrime] = wmwc.gf.add(tableTotalSum[kPrime][rPrime],
                            tableForNode[kPrime][rPrime]);
                }
            }

        }

        weightedVData.compDuration += System.currentTimeMillis() - t;

        if (iter == (wmwc.twoRaisedToK -1) && localSS == (wmwc.workerSteps -1)){
            t = System.currentTimeMillis();
            // Now, we can change the totalSumTable to the decisionTable
            int[][] tableTotalSum = weightedVData.totalSumTable;
            for (int kPrime = 0; kPrime <= k; kPrime++) {
                for (int rPrime = 0; rPrime <= weightedVData.r; rPrime++) {
                    tableTotalSum[kPrime][rPrime] = (tableTotalSum[kPrime][rPrime] > 0) ? 1 : -1;
                }
            }
            DoubleArrayListWritable nodeLocalBestScoreInfo = getScoreFromTablePower(tableTotalSum, wmwc.alphaMax, weightedVData.vertexId);
            weightedVData.compDuration += System.currentTimeMillis() - t;
            aggregate(WeightedMultilinearMaster.W_MULTILINEAR_RESULT, nodeLocalBestScoreInfo);
            aggregate(WeightedMultilinearMaster.W_MULTILINEAR_COMP_TIME, new LongWritable(weightedVData.compDuration));
            vertex.voteToHalt();
        }
    }

    @Override
    public void preSuperstep() {
        wmwc = getWorkerContext();
        roundingFactor = 1+wmwc.delta;
        k = wmwc.k;
    }

    // This is node local best score, not the global best
    // to get the global best we have to find the max of these local best scores,
    // which we'll do in the master using aggregation
    public DoubleArrayListWritable getScoreFromTablePower(int[][] existenceForNode, double alpha, int vertexId) {
        double nodeLocalBestScore = -1;
        int nodeLocalBestScoreK = -1;
        int nodeLocalBestScoreR = -1;
        for (int kPrime = 1; kPrime < existenceForNode.length; kPrime++) {
            for (int rPrime = 0; rPrime < existenceForNode[0].length; rPrime++) {
                if (existenceForNode[kPrime][rPrime] == 1) {
                    //System.out.println("Found subgraph with size " + kPrime + " and score " + rPrime);
                    // size cannot be smaller than weight
                    int unroundedPrize = (int) Math.pow(roundingFactor, rPrime - 1);
                    // adjust for the fact that this is the refined graph
                    int unroundedSize = Math.max(kPrime, unroundedPrize);
                    double score = Utils.BJ(alpha, unroundedPrize, unroundedSize);
                    //System.out.println("Score is " + score);
                    if (score > nodeLocalBestScore){
                        nodeLocalBestScore = score;
                        nodeLocalBestScoreK = kPrime;
                        nodeLocalBestScoreR = rPrime;
                    }
                }
            }
        }

        DoubleArrayListWritable ret = new DoubleArrayListWritable();
        ret.addDouble(vertexId);
        ret.addDouble(nodeLocalBestScoreK);
        ret.addDouble(nodeLocalBestScoreR);
        ret.addDouble(nodeLocalBestScore);

        return ret;
    }

}
