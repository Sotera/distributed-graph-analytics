package com.soteradefense.dga.highbetweenness;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class StringWritable {

    protected String readString(DataInput in) throws IOException {
        int len = in.readInt();
        byte[] stringInBytes = new byte[len];
        in.readFully(stringInBytes, 0, len);
        return new String(stringInBytes);
    }

    protected void writeString(DataOutput out, String val) throws IOException {
        out.writeInt(val.length());
        out.writeBytes(val);
    }
}
