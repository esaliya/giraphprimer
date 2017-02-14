package org.saliya.giraphprimer;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Saliya Ekanayake on 2/5/17.
 */
public class IntArrayWritable implements Writable {
    private int[] data;
    private int startIdx;
    private int endIdxInclusive;
    private byte serializedFlag = 0;

    public IntArrayWritable(){

    }

    public IntArrayWritable(int[] data, int startIdx, int endIdxInclusive){
        this.data = data;
        this.startIdx = startIdx;
        this.endIdxInclusive = endIdxInclusive;
    }

    public int[] getData(){
        return data;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        if (serializedFlag == 0) {
            int length = 0;
            if(data != null) {
                // +2 because we need one extra to write vertexId at the end
                length = (endIdxInclusive - startIdx)+2;
            }
            out.writeInt(length);
            // i <= endIdxInclusive because we only want
            // that much of data. The last element of the
            // outgoing message will be the vertexId
            for (int i = startIdx; i <= endIdxInclusive; i++) {
                out.writeInt(data[i]);
            }
            // write vertexId
            out.writeInt(data[data.length - 1]);
        } else {
            // Serializing an already serialized content
            // So write everything out
            out.writeInt(data.length);
            for (int i = 0; i < data.length; ++i){
                out.writeInt(data[i]);
            }
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        serializedFlag = 1; // i.e. read serialized content
        int length = in.readInt();

        data = new int[length];

        for(int i = 0; i < length; i++) {
            data[i] = in.readInt();
        }
    }
}
