package org.saliya.giraphprimer.multilinear.giraph;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Saliya Ekanayake on 2/16/17.
 */
public class IntMatrixWritable implements Writable {
    private int[][] data;
    private int vertexId;

    public IntMatrixWritable(){

    }

    public IntMatrixWritable(int[][] data, int vertexId){
        this.data = data;
        this.vertexId = vertexId;
    }

    public int[][] getData(){
        return data;
    }
    public int getVertexId() {return vertexId;}

    @Override
    public void write(DataOutput out) throws IOException {
        int dimALen = 0;
        if(data != null && data.length > 0) {
            dimALen = data.length;
        }

        int dimBLen = 0;
        if (dimALen > 0){
            dimBLen = data[0].length;
        }

        out.writeInt(vertexId);
        out.writeInt(dimALen);
        out.writeInt(dimBLen);

        if (dimALen > 0 && dimBLen > 0) {
            for (int i = 0; i < dimALen; i++) {
                for (int j = 0; j < dimBLen; ++j) {
                    out.writeInt(data[i][j]);
                }
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        vertexId = in.readInt();
        int dimALen = in.readInt();
        int dimBLen = in.readInt();

        if (dimALen > 0 && dimBLen > 0) {
            data = new int[dimALen][dimBLen];

            for (int i = 0; i < dimALen; i++) {
                for (int j = 0; j < dimBLen; ++j) {
                    data[i][j] = in.readInt();
                }
            }
        }
    }
}
