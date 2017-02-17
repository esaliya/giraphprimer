package org.saliya.giraphprimer.multilinear.giraph.weighted;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

/**
 * Saliya Ekanayake on 2/15/17.
 */
public class WeightedVData implements WritableComparable<WeightedVData>{
    public int vertexId = -1;
    public double vertexWeight = -1.0;
    public int[][] optTable = null;
    public int[][] extTable = null;
    public int[][] totalSumTable = null;
    public int[] cumulativeCompletionVariables = null;
    public long uniqueRandomSeed;
    public Random random;
    public int r;

    public WeightedVData(){}

    public WeightedVData(int vertexId, double vertexWeight){
        this.vertexId = vertexId;
        this.vertexWeight = vertexWeight;
    }

    @Override
    public int compareTo(WeightedVData o) {
        return (vertexId < o.vertexId ? -1 : (vertexId == o.vertexId ? 0 : 1));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(vertexId);
        out.writeDouble(vertexWeight);
        out.writeInt(r);
        int dimALen = (optTable != null) ? optTable.length : 0;
        int dimBLen = (optTable != null && optTable.length > 0) ? optTable[0].length : 0;
        out.writeInt(dimALen);
        out.writeInt(dimBLen);
        out.writeInt(((cumulativeCompletionVariables != null && cumulativeCompletionVariables.length > 0) ?
                cumulativeCompletionVariables.length : 0));

        if (optTable!=null && optTable.length > 0) {
            for (int[] arr : optTable) {
                for (int a : arr) {
                    out.writeInt(a);
                }
            }
        }
        if (extTable != null && extTable.length > 0) {
            for (int[] arr : extTable) {
                for (int a : arr) {
                    out.writeInt(a);
                }
            }
        }
        if (totalSumTable != null && totalSumTable.length > 0) {
            for (int[] arr : totalSumTable) {
                for (int a : arr) {
                    out.writeInt(a);
                }
            }
        }
        if (cumulativeCompletionVariables != null && cumulativeCompletionVariables.length > 0){
            for (int cumulativeCompletionVariable : cumulativeCompletionVariables) {
                out.writeInt(cumulativeCompletionVariable);
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        vertexId = in.readInt();
        vertexWeight = in.readDouble();
        r = in.readInt();
        int dimALen = in.readInt();
        int dimBLen = in.readInt();
        int cumulativeLen = in.readInt();

        if (dimALen > 0 && dimBLen > 0)
        optTable = new int[dimALen][dimBLen];
        extTable = new int[dimALen][dimBLen];
        totalSumTable = new int[dimALen][dimBLen];

        for (int i = 0; i < dimALen; ++i){
            for (int j = 0; j < dimBLen; ++j){
                optTable[i][j] = in.readInt();
            }
        }
        for (int i = 0; i < dimALen; ++i){
            for (int j = 0; j < dimBLen; ++j){
                extTable[i][j] = in.readInt();
            }
        }
        for (int i = 0; i < dimALen; ++i){
            for (int j = 0; j < dimBLen; ++j){
                totalSumTable[i][j] = in.readInt();
            }
        }

        if (cumulativeLen > 0){
            cumulativeCompletionVariables = new int[cumulativeLen];
            for (int i = 0; i < cumulativeLen; ++i){
                cumulativeCompletionVariables[i] = in.readInt();
            }
        }
    }

    /**
     * Returns true iff <code>o</code> is a {@link WeightedVData} with the same vertexId.
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof WeightedVData)) {
            return false;
        }
        WeightedVData other = (WeightedVData)o;
        return this.vertexId == other.vertexId;
    }

    @Override
    public int hashCode() {
        return vertexId;
    }

    /** A Comparator optimized for DoubleWritable. */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(WeightedVData.class);
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
        WritableComparator.define(WeightedVData.class, new WeightedVData.Comparator());
    }
}
