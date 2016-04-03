package org.tkorostelev.homework03;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Point04Writable implements Writable {

    private IntWritable ipOccurences;
    private DoubleWritable biddingSum;

    public Point04Writable() {
        set(new IntWritable(), new DoubleWritable());
    }

    public Point04Writable(IntWritable ipOccurences, DoubleWritable biddingSum) {
        set(ipOccurences, biddingSum);
    }

    public void set(IntWritable ipOccurences, DoubleWritable biddingSum) {
        this.ipOccurences = ipOccurences;
        this.biddingSum = biddingSum;
    }

    public IntWritable getIpOccurences() {
        return ipOccurences;
    }

    public DoubleWritable getBiddingSum() {
        return biddingSum;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        ipOccurences.write(out);
        biddingSum.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ipOccurences.readFields(in);
        biddingSum.readFields(in);
    }

    @Override
    public int hashCode() {
        return ipOccurences.hashCode() * 163 + biddingSum.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Point04Writable) {
            Point04Writable tp = (Point04Writable) o;
            return ipOccurences.equals(tp.ipOccurences) && biddingSum.equals(tp.biddingSum);
        }
        return false;
    }

    @Override
    public String toString() {
        return ipOccurences.toString() + "\t" + biddingSum.toString();
    }
    
}