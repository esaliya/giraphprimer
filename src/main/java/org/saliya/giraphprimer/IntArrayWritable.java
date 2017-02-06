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

    public IntArrayWritable(){

    }

    public IntArrayWritable(int[] data){
        this.data = data;
    }

    public int[] getData(){
        return data;
    }

    public void setData(int[] data){
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
            out.writeInt(data[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();

        data = new int[length];

        for(int i = 0; i < length; i++) {
            data[i] = in.readInt();
        }
    }
}
