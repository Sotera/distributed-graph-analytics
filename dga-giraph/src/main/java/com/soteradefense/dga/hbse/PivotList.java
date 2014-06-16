package com.soteradefense.dga.hbse;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;


public class PivotList implements Writable {

    private List<String> pivots;

    public PivotList() {
        super();
        pivots = new LinkedList<String>();
    }

    public PivotList(String s) {
        this();
        pivots.add(s);
    }

    public void aggregate(PivotList other) {
        if(!pivots.containsAll(other.pivots))
            pivots.addAll(other.pivots);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(pivots.size());
        for (String s : pivots) {
            Text.writeString(dataOutput, s);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pivots.clear();

        int size = dataInput.readInt();
        for (int i = 0; i < size; ++i) {
            pivots.add(Text.readString(dataInput));
        }
    }

    public List<String> getPivots() {
        return pivots;
    }

    public void trim(int totalLength) {
        pivots = pivots.subList(0, totalLength);
    }
}
