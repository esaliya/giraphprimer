package org.saliya.giraphprimer.multilinear.giraph;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Saliya Ekanayake on 2/15/17.
 */
public class DoubleArrayListWritable implements Writable {
    private ArrayList<Double> data;

    public DoubleArrayListWritable(){
        data = new ArrayList<>();
    }

    public DoubleArrayListWritable(double d){
        this();
        data.add(d);
    }

    public ArrayList<Double> getData() { return data; }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(data.size());
        for (double d: data){
            out.writeDouble(d);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        data = new ArrayList<>(size);
        for (int i = 0; i < size; ++i){
            data.add(in.readDouble());
        }

    }
}
