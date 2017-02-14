package org.saliya.giraphprimer;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Saliya Ekanayake on 2/5/17.
 */
public class IntArrayWritable implements Writable {
    private ByteBuffer buffer;
    private int length;
    private int startIdx;
    private int endIdxInclusive;
    private byte serializedFlag = 0;

    public IntArrayWritable(){

    }

    public IntArrayWritable(int[] data, int startIdx, int endIdxInclusive){
        this.startIdx = startIdx;
        this.endIdxInclusive = endIdxInclusive;
        // +3 because we need
        // one extra to write vertexId at the end
        // one to write length at the beginning
        length = (endIdxInclusive - startIdx)+3;
        buffer = ByteBuffer.allocate(length*Integer.BYTES);
        buffer.position(0);
        buffer.putInt(length);
        // i <= endIdxInclusive because we only want
        // that much of data. The last element of the
        // outgoing message will be the vertexId
        for (int i = startIdx; i <= endIdxInclusive; i++) {
            buffer.putInt(data[i]);
        }
        // write vertexId
        buffer.putInt(data[data.length - 1]);
    }

    public int get(int i){
        return buffer.getInt(i*Integer.BYTES);
    }

    public int getLength(){
        return length;
    }


    @Override
    public void write(DataOutput out) throws IOException {
//        if (serializedFlag == 0) {
//            int length = 0;
//            if(data != null) {
//                // +2 because we need one extra to write vertexId at the end
//                length = (endIdxInclusive - startIdx)+2;
//            }
//            out.writeInt(length);
//            // i <= endIdxInclusive because we only want
//            // that much of data. The last element of the
//            // outgoing message will be the vertexId
//            for (int i = startIdx; i <= endIdxInclusive; i++) {
//                out.writeInt(data[i]);
//            }
//            // write vertexId
//            out.writeInt(data[data.length - 1]);
//        } else {
//            // Serializing an already serialized content
//            // So write everything out
//            out.writeInt(data.length);
//            for (int i = 0; i < data.length; ++i){
//                out.writeInt(data[i]);
//            }
//        }
        out.writeInt(length);
        out.write(buffer.array(), Integer.BYTES, (length-1)*Integer.BYTES);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        length = in.readInt();
        buffer = ByteBuffer.allocate(length*Integer.BYTES);
        buffer.position(0);
        buffer.putInt(length);
        in.readFully(buffer.array(), Integer.BYTES, (length-1)*Integer.BYTES);
    }
}
