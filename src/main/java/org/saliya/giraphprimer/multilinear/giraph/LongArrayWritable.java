package org.saliya.giraphprimer.multilinear.giraph;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Saliya Ekanayake on 2/5/17.
 */
public class LongArrayWritable implements Writable {
    private long[] data;

    public LongArrayWritable(){

    }

    public LongArrayWritable(long[] data){
        this.data = data;
    }

    public long[] getData(){
        return data;
    }

    public void setData(long[] data){
        this.data = data;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int length = 0;
        if(data != null) {
            length = data.length;
        }

        out.writeInt(length);

        for(int i = 0; i < length; i++) {
            out.writeLong(data[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();

        data = new long[length];

        for(int i = 0; i < length; i++) {
            data[i] = in.readLong();
        }
    }
}
