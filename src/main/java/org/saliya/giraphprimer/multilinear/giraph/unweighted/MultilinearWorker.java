package org.saliya.giraphprimer.multilinear.giraph.unweighted;


import org.apache.giraph.Algorithm;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.*;
import org.saliya.giraphprimer.multilinear.giraph.LongArrayWritable;
import org.saliya.giraphprimer.multilinear.giraph.ShortArrayWritable;

import java.io.IOException;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.IntStream;

public class MultilinearWorker extends BasicComputation<
        IntWritable, VData, NullWritable, ShortArrayWritable> {

    private MultilinearWorkerContext mwc;
    private long computeTime = 0L;
    private long sortTime = 0L;

    @Override
    public void compute(
            Vertex<IntWritable, VData, NullWritable> vertex,
            Iterable<ShortArrayWritable> messages) throws IOException {

        VData vData = vertex.getValue();


        long ss = getSuperstep();
        int localSS = (int)ss % MultilinearWorkerContext.workerSteps;
        // The external loop number that goes from 0 to twoRaisedToK (excluding)
        int iter = (int)ss / MultilinearWorkerContext.workerSteps;
        if (ss == 0){
            // Set array in vertex data
            vData.vertexRow = new short[mwc.k+1];
            vData.vertexRowLength = vData.vertexRow.length;
            vData.randomWeightToComputeCircuitSum = getRandomWeightToComputeCircuitSum(
                    new Random(mwc.randomSeed), MultilinearWorkerContext.fieldSize, vData.vertexId);
            long[] nums = this.<LongArrayWritable>getBroadcast(MultilinearMaster.MULTILINEAR_RANDOM_NUMS).getData();
            vData.uniqueRandomSeed = nums[vData.vertexId];
        }
        // In the original code I started from 2 and went till k (including)
        // we start localSS from zero, which is for initialization
        // then real work begins with localSS=1, which should give I=2, hence I=localSS+1
        int I = localSS+1;
        if (localSS == 0){
            vData.random = new Random(vData.uniqueRandomSeed);
            IntStream.range(0, vData.vertexRowLength).forEach(x -> vData.vertexRow[x] = 0);
            int color = vData.vertexColor;
            int dotProduct = mwc.randomAssignment[color] & iter;
            vData.vertexRow[1] = (short)((Integer.bitCount(dotProduct) % 2 == 1) ? 0 : 1);

        } else {
            // business logic only if localSS != 0

            // handy test code
           /* if (vertex.getId().get() == 1) {
                System.out.println("Vertex: " + vertex.getId().get() + " i: " + I + " ss: " + ss + " localSS: " + localSS + " iter: " + iter);
            }*/

            long t = System.currentTimeMillis();
            TreeMap<Integer, ShortArrayWritable> messagesSortedByVertexId = new TreeMap<>();
            for (ShortArrayWritable message: messages){
                // Now this data length is < vData.vertexRowLength
                // because we only send the required elements for
                // this iteration.
                messagesSortedByVertexId.put(message.getVertexId(), message);
            }
            sortTime += System.currentTimeMillis() - t;

            t = System.currentTimeMillis();
            Set<Integer> neighborsInAscendingOrder = messagesSortedByVertexId.keySet();
            for (int j = 1; j < I; j++) {
                for (int neighbor: neighborsInAscendingOrder) {
                    int weight = vData.random.nextInt(MultilinearWorkerContext.fieldSize);
                    int product = MultilinearWorkerContext.gf.ffMultiply(vData.vertexRow[j], messagesSortedByVertexId.get(neighbor).get(I - j));
                    product = MultilinearWorkerContext.gf.ffMultiply(weight, product);
                    vData.vertexRow[I] = (short) MultilinearWorkerContext.gf.add(vData.vertexRow[I], product);
                }
            }
            computeTime += System.currentTimeMillis() - t;
        }

        if (localSS != (MultilinearWorkerContext.workerSteps -1)){
            // This message will be used in the I+1 iteration,
            // Let nextI = I+1, then we only need to send
            // elements from [1, nextI-1]
            int nextI = I+1;
            ShortArrayWritable message = new ShortArrayWritable(vData.vertexRow, vData.vertexId, 1, nextI-1);
            sendMessageToAllEdges(vertex, message);

        } else {
            aggregate(MultilinearMaster.MULTILINEAR_CIRCUIT_SUM,
                    new IntWritable(MultilinearWorkerContext.gf.ffMultiply(
                            vData.randomWeightToComputeCircuitSum, vData.vertexRow[mwc.k])));
        }


        if (iter == (MultilinearWorkerContext.twoRaisedToK -1) && localSS == (MultilinearWorkerContext.workerSteps -1)){
            aggregate(MultilinearMaster.MULTILINEAR_SORT_TIME, new LongWritable(sortTime));
            aggregate(MultilinearMaster.MULTILINEAR_COMPUTE_TIME, new LongWritable(computeTime));
            vertex.voteToHalt();
        }
    }

    @Override
    public void preSuperstep() {
        mwc = getWorkerContext();
    }

    private int getRandomWeightToComputeCircuitSum(Random random, int fieldSize, int vertexId){
        for (int i = 0; i < vertexId; ++i){
            random.nextInt(fieldSize); // waste these
        }
        return random.nextInt(fieldSize);
    }
}
