package org.saliya.giraphprimer.withmaster;


import org.apache.giraph.Algorithm;
import org.apache.giraph.GiraphRunner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
@Algorithm(
        name = "Shortest paths",
        description = "Finds all shortest paths from a selected vertex"
)
public class GiraphTestWorker extends BasicComputation<
        LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    @Override
    public void compute(
            Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
            Iterable<DoubleWritable> messages) throws IOException {

        long superStep = getSuperstep();
        StringBuffer sb = new StringBuffer();
        for (DoubleWritable message : messages) {
            sb.append(message.get() + " ");
        }

        double doubleSum = this.<DoubleWritable>getAggregatedValue(GiraphTestMaster.doubleSumString).get();
        double doubleBcast = this.<DoubleWritable>getBroadcast(GiraphTestMaster.doubleBcastString).get();
        System.out.println("SuperStep: " + superStep + " Vertex: " + vertex.getId() + " received " + sb.toString() + " my.conf.param=" + getConf().getInt("my.conf.param",0) + " aggregated recvd for vertex: " + doubleSum + " bcast recvd for verted: " + doubleBcast);

        if (superStep < 2) {
            aggregate(GiraphTestMaster.doubleSumString, new DoubleWritable(1.0));
            for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
                sendMessage(edge.getTargetVertexId(), new DoubleWritable(vertex.getId().get()));
            }
        }


        vertex.voteToHalt();
    }


}
