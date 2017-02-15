package org.saliya.giraphprimer.multilinear.giraph;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Saliya Ekanayake on 2/5/17.
 */
public class ShortArrayWritable implements Writable {
    private ByteBuffer buffer;
    private short length;
    private int vertexId;

    public ShortArrayWritable(){

    }

    public ShortArrayWritable(short[] data, int vertexId, int startIdx, int endIdxInclusive){
        // +3 because we need
        // one extra to write vertexId at the end
        // one to write length at the beginning
        // Remember, the last element, i.e. the vertexId, is Int
        length = (short)((endIdxInclusive - startIdx)+3);
        buffer = ByteBuffer.allocate((length-1)*Short.BYTES+Integer.BYTES);
        buffer.position(0);
        buffer.putShort(length);
        // i <= endIdxInclusive because we only want
        // that much of data. The last element of the
        // outgoing message will be the vertexId
        for (int i = startIdx; i <= endIdxInclusive; i++) {
            buffer.putShort(data[i]);
        }
        // write vertexId
        this.vertexId = vertexId;
        buffer.putInt(vertexId);
    }

    public int getVertexId(){
        return vertexId;
    }

    public short get(int i){
        return buffer.getShort(i*Short.BYTES);
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
        out.writeShort(length);
        out.write(buffer.array(), Short.BYTES, (length-2)*Short.BYTES+Integer.BYTES);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        length = in.readShort();
        buffer = ByteBuffer.allocate((length-1)*Short.BYTES+Integer.BYTES);
        buffer.position(0);
        buffer.putShort(length);
        in.readFully(buffer.array(), Short.BYTES, (length-2)*Short.BYTES+Integer.BYTES);
        vertexId = buffer.getInt((length-1)*Short.BYTES);
    }
}
