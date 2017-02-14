package org.saliya.giraphprimer.withmaster.customdata;


import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.*;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import org.saliya.giraphprimer.IntArrayWritable;
import org.saliya.giraphprimer.LongArrayWritable;
import org.saliya.giraphprimer.VData;

import javax.swing.plaf.synth.SynthTextAreaUI;
import java.io.IOException;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.IntStream;

import static org.saliya.giraphprimer.withmaster.customdata.MultilinearWorkerContext.fieldSize;
import static org.saliya.giraphprimer.withmaster.customdata.MultilinearWorkerContext.gf;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
@Algorithm(
        name = "Shortest paths",
        description = "Finds all shortest paths from a selected vertex"
)
public class MultilinearWorker extends BasicComputation<
        IntWritable, VData, NullWritable, IntArrayWritable> {

    private MultilinearWorkerContext mwc;
    private long computeTime = 0L;
    private long sortTime = 0L;

    @Override
    public void compute(
            Vertex<IntWritable, VData, NullWritable> vertex,
            Iterable<IntArrayWritable> messages) throws IOException {

        VData vData = vertex.getValue();


        long ss = getSuperstep();
        int localSS = (int)ss % MultilinearWorkerContext.workerSteps;
        // The external loop number that goes from 0 to twoRaisedToK (excluding)
        int iter = (int)ss / MultilinearWorkerContext.workerSteps;
        if (ss == 0){
            // Set array in vertex data
            // We'll use the last element, i.e. k+1 idx, to keep track of the vertex id
            vData.vertexRow = new int[mwc.k+2];
            vData.vertexRowLength = vData.vertexRow.length;
            vData.randomWeightToComputeCircuitSum = getRandomWeightToComputeCircuitSum(
                    new Random(mwc.randomSeed), fieldSize, vData.vertexId);
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
            // Set the last element to the vertex id, so when messages are collected
            // we can find which vertex it belongs to
            vData.vertexRow[vData.vertexRowLength-1] = vData.vertexId;
            int color = vData.vertexColor;
            int dotProduct = mwc.randomAssignment[color] & iter;
            vData.vertexRow[1] = (Integer.bitCount(dotProduct) % 2 == 1) ? 0 : 1;

        } else {
            // business logic only if localSS != 0

            // handy test code
           /* if (vertex.getId().get() == 1) {
                System.out.println("Vertex: " + vertex.getId().get() + " i: " + I + " ss: " + ss + " localSS: " + localSS + " iter: " + iter);
            }*/

            long t = System.currentTimeMillis();
            TreeMap<Integer, int[]> messagesSortedByVertexId = new TreeMap<>();
            for (IntArrayWritable message: messages){
                int [] data = message.getData();
                // Now this data length is < vData.vertexRowLength
                // because we only send the required elements for
                // this iteration.
                messagesSortedByVertexId.put(data[data.length-1], data);
            }
            sortTime += System.currentTimeMillis() - t;

            t = System.currentTimeMillis();
            Set<Integer> neighborsInAscendingOrder = messagesSortedByVertexId.keySet();
            for (int j = 1; j < I; j++) {
                for (int neighbor: neighborsInAscendingOrder) {
                    int weight = vData.random.nextInt(fieldSize);
                    // (I-j)-1 because now we want the original
                    // (I-j) values to map to the one before the last (last element is vertexId)
                    // element of the message array and so on (happens in descending order)
                    int product = gf.ffMultiply(vData.vertexRow[j], messagesSortedByVertexId.get(neighbor)[(I - j)-1]);
                    product = gf.ffMultiply(weight, product);
                    vData.vertexRow[I] = gf.add(vData.vertexRow[I], product);
                }
            }
            computeTime += System.currentTimeMillis() - t;
        }

        if (localSS != (MultilinearWorkerContext.workerSteps -1)){
            // This message will be used in the I+1 iteration,
            // Let nextI = I+1, then we only need to send
            // elements from [1, nextI-1]
            int nextI = I+1;
            IntArrayWritable message = new IntArrayWritable(vData.vertexRow, 1, nextI-1);
            sendMessageToAllEdges(vertex, message);

        } else {
            aggregate(MultilinearMaster.MULTILINEAR_CIRCUIT_SUM,
                    new IntWritable(gf.ffMultiply(
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
