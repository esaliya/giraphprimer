package org.saliya.giraphprimer.withmaster.customdata;


import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.*;
import org.saliya.giraphprimer.IntArrayWritable;
import org.saliya.giraphprimer.VData;

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
        }
        // In the original code I started from 2 and went till k (including)
        // we start localSS from zero, which is for initialization
        // then real work begins with localSS=1, which should give I=2, hence I=localSS+1
        int I = localSS+1;
        if (localSS == 0){
            vData.random = new Random(mwc.randomSeed);
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

            TreeMap<Integer, int[]> messagesSortedByVertexId = new TreeMap<>();
            for (IntArrayWritable message: messages){
                int [] data = message.getData();
                messagesSortedByVertexId.put(data[vData.vertexRowLength-1], data);
            }

            Set<Integer> neighborsInAscendingOrder = messagesSortedByVertexId.keySet();
            for (int j = 1; j < I; j++) {
                for (int neighbor: neighborsInAscendingOrder) {
                    int weight = vData.random.nextInt(fieldSize);
                    int product = gf.ffMultiply(vData.vertexRow[j], messagesSortedByVertexId.get(neighbor)[I - j]);
                    product = gf.ffMultiply(weight, product);
                    vData.vertexRow[I] = gf.add(vData.vertexRow[I], product);
                }
            }
        }

        if (localSS != (MultilinearWorkerContext.workerSteps -1)){
            for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {
                sendMessage(edge.getTargetVertexId(), new IntArrayWritable(vData.vertexRow));
            }

        } else {
            aggregate(MultilinearWorkerContext.MultilinearMaster.MULTILINEAR_CIRCUIT_SUM,
                    new IntWritable(gf.ffMultiply(
                            vData.randomWeightToComputeCircuitSum, vData.vertexRow[mwc.k])));
        }


        if (iter == (MultilinearWorkerContext.twoRaisedToK -1) && localSS == (MultilinearWorkerContext.workerSteps -1)){
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
