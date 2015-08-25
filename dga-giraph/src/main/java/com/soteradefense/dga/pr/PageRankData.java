package com.soteradefense.dga.pr;

import org.apache.hadoop.io.Writable;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

/**
 * Created by ekimbrel on 8/25/15.
 */
public class PageRankData implements Writable{

    public double rank;
    public double delta;

    public PageRankData(){
        this.rank=1.0;
        this.delta=1.0;
    }

    public PageRankData(double rank,double delta){
        this.rank = rank;
        this.delta = delta;
    }

    public void write(DataOutput out) throws IOException{
        out.writeDouble(this.rank);
        out.writeDouble(this.delta);
    }

    public void readFields(DataInput in) throws IOException{
        this.rank = in.readDouble();
        this.delta = in.readDouble();
    }

    public static PageRankData read(DataInput in) throws IOException{
        PageRankData d = new PageRankData();
        d.readFields(in);
        return d;
    }
}
