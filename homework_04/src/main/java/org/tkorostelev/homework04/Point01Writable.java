package org.tkorostelev.homework04;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Point01Writable implements WritableComparable<Point01Writable> {

    private Text iPinYouID;
    private Text timestamp;

    public Point01Writable() {
        set(new Text(), new Text());
    }

    public Point01Writable(String first, String timestamp) {
        set(new Text(iPinYouID), new Text(timestamp));
    }

    public Point01Writable(Text iPinYouID, Text timestamp) {
        set(iPinYouID, timestamp);
    }

    public void set(Text iPinYouID, Text timestamp) {
        this.iPinYouID = iPinYouID;
        this.timestamp = timestamp;
    }

    public Text getiPinYouID() {
        return iPinYouID;
    }

    public Text getTimestamp() {
        return timestamp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        iPinYouID.write(out);
        timestamp.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        iPinYouID.readFields(in);
        timestamp.readFields(in);
    }

    @Override
    public int hashCode() {
        return iPinYouID.hashCode() * 163 + timestamp.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Point01Writable) {
            Point01Writable tp = (Point01Writable) o;
            return iPinYouID.equals(tp.iPinYouID) && timestamp.equals(tp.timestamp);
        }
        return false;
    }

    @Override
    public String toString() {
        return iPinYouID + "\t" + timestamp;
    }

    @Override
    public int compareTo(Point01Writable tp) {
        int cmp = iPinYouID.compareTo(tp.iPinYouID);
        if (cmp != 0) {
            return cmp;
        }
        return timestamp.compareTo(tp.timestamp);
    }

    public static class Comparator extends WritableComparator {

        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public Comparator() {
            super(Point01Writable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            try {
                int iPinYouIDL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int iPinYouIDL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                int cmp = TEXT_COMPARATOR.compare(b1, s1, iPinYouIDL1, b2, s2, iPinYouIDL2);
                if (cmp != 0) {
                    return cmp;
                }
                return TEXT_COMPARATOR.compare(b1, s1 + iPinYouIDL1, l1 - iPinYouIDL1,
                        b2, s2 + iPinYouIDL2, l2 - iPinYouIDL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    static {
        WritableComparator.define(Point01Writable.class, new Comparator());
    }

    public static class iPinYouIDComparator extends WritableComparator {

        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public iPinYouIDComparator() {
            super(Point01Writable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            try {
                int iPinYouIDL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int iPinYouIDL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                return TEXT_COMPARATOR.compare(b1, s1, iPinYouIDL1, b2, s2, iPinYouIDL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof Point01Writable && b instanceof Point01Writable) {
                return ((Point01Writable) a).iPinYouID.compareTo(((Point01Writable) b).iPinYouID);
            }
            return super.compare(a, b);
        }
    }

}
