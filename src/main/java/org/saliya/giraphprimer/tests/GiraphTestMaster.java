package org.saliya.giraphprimer.tests.withmaster;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;

/**
 * Saliya Ekanayake on 2/2/17.
 */
public class GiraphTestMaster  extends DefaultMasterCompute{
    static final String doubleBcastString = "double.bcast.string";
    static final String doubleSumString = "double.sum.string";

    @Override
    public void compute() {
        long masterSuperStep = getSuperstep();
        double doubleSum = this.<DoubleWritable>getAggregatedValue(doubleSumString).get();
        this.broadcast(doubleBcastString, new DoubleWritable(-1.0));
        System.out.println("Master SuperStep: " + masterSuperStep + " aggregated value: " + doubleSum);

    }

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        registerAggregator(doubleSumString, DoubleSumAggregator.class);
    }
}
