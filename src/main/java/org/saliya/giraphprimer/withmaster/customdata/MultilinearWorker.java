package org.saliya.giraphprimer.withmaster.customdata;


import com.google.common.base.Strings;
import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.*;
import org.saliya.giraphprimer.VData;

import java.io.IOException;
import java.util.stream.IntStream;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
@Algorithm(
        name = "Shortest paths",
        description = "Finds all shortest paths from a selected vertex"
)
public class MultilinearWorker extends BasicComputation<
        IntWritable, VData, NullWritable, DoubleWritable> {
    private MultilinearWorkerContext mwc;

    @Override
    public void compute(
            Vertex<IntWritable, VData, NullWritable> vertex,
            Iterable<DoubleWritable> messages) throws IOException {

        long ss = getSuperstep();
        int localSS = (int)ss % MultilinearWorkerContext.workerSteps;
        // The external loop number that goes from 0 to twoRaisedToK (excluding)
        int iter = (int)ss / MultilinearWorkerContext.workerSteps;
        if (ss == 0){
            // Set array in vertex data
            VData vData = vertex.getValue();
            // We'll use the last element, i.e. k+1 idx, to keep track of the vertex id
            vData.vertexRow = new int[mwc.k+2];
            IntStream.range(0, vData.vertexRow.length).forEach(i -> vData.vertexRow[i] = 0);
            // Set the last element to the vertex id, so when messages are collected
            // we can find which vertex it belongs to
            vData.vertexRow[mwc.k+1] = vData.vertexId;
            int color = vData.vertexColor;
            int dotProduct = mwc.randomAssignment[color] & iter;
            vData.vertexRow[1] = (Integer.bitCount(dotProduct) % 2 == 1) ? 0 : 1;
        }
        // In the original code i started from 2 and went till k (including)
        // we start localSS from zero, which is for initialization
        // then real work begins with localSS=1, which should give i=2, hence i=localSS+1
        int i = localSS+1;
        if (localSS == 0){
            VData vData = vertex.getValue();
            int [] vertexRow = vData.vertexRow;
            int vertexRowLength = vData.vertexRowLength;

        } else {
            // business logic only if localSS != 0
            if (vertex.getId().get() == 1) {
                System.out.println("Vertex: " + vertex.getId().get() + " i: " + i + " ss: " + ss + " localSS: " + localSS + " iter: " + iter);
            }
        }

        if (localSS != (MultilinearWorkerContext.workerSteps -1)){
            for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {
                sendMessage(edge.getTargetVertexId(), new DoubleWritable(vertex.getId().get()));
            }
        } else {
            // set aggregate
        }


        if (iter == (mwc.twoRaisedToK-1) && localSS == (MultilinearWorkerContext.workerSteps -1)){
            vertex.voteToHalt();
        }
    }

    @Override
    public void preSuperstep() {
        mwc = getWorkerContext();
    }
}
