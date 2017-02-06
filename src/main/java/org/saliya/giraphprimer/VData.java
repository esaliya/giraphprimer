package org.saliya.giraphprimer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

/**
 * Saliya Ekanayake on 1/31/17.
 */
public class VData implements WritableComparable<VData> {
    public int vertexId = -1;
    public int vertexColor = -1;
    public int vertexRowLength = -1;
    public int[] vertexRow = null;
    public int randomWeightToComputeCircuitSum;
    public Random random;

    public VData(){}

    public VData(int vertexId, int vertexColor){
        this.vertexId = vertexId;
        this.vertexColor = vertexColor;
    }

    @Override
    public int compareTo(VData o) {
        return (vertexId < o.vertexId ? -1 : (vertexId == o.vertexId ? 0 : 1));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        System.out.println("write called for v: " + vertexId);
        out.writeInt(vertexId);
        out.writeInt(vertexColor);
        out.writeInt(randomWeightToComputeCircuitSum);
        out.writeInt(vertexRowLength);
        for (int i = 0; i < vertexRowLength; ++i){
            out.writeInt(vertexRow[i]);
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        vertexId = in.readInt();
        System.out.println("read called for v: " + vertexId);
        vertexColor = in.readInt();
        randomWeightToComputeCircuitSum = in.readInt();
        vertexRowLength = in.readInt();
        for (int i = 0; i < vertexRowLength; ++i){
            vertexRow[i] = in.readInt();
        }
    }

    /**
     * Returns true iff <code>o</code> is a {@link VData} with the same vertexId.
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof VData)) {
            return false;
        }
        VData other = (VData)o;
        return this.vertexId == other.vertexId;
    }

    @Override
    public int hashCode() {
        return vertexId;
    }

    /** A Comparator optimized for DoubleWritable. */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(VData.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            double thisVertexId = readInt(b1, s1);
            double thatVertexId = readInt(b2, s2);
            return (thisVertexId < thatVertexId ? -1 : (thisVertexId == thatVertexId ? 0 : 1));
        }
    }

    static {                                        // register this comparator
        WritableComparator.define(VData.class, new VData.Comparator());
    }
}
