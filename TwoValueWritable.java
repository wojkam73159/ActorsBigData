
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class TwoValueWritable  implements WritableComparable<TwoValueWritable> {//extends IntWritable
    IntWritable first;
    IntWritable second;

    public  TwoValueWritable() {
        set(new IntWritable(0), new IntWritable(0));
    }
    public  TwoValueWritable(Integer fir, Integer sec) {
        set(new IntWritable(fir), new IntWritable(sec));
    }


    public void set(IntWritable a, IntWritable b) {
        this.first = a;
        this.second = b;
    }
    public IntWritable getFirst() {
        return first;
    }
    public IntWritable getSecond() {
        return second;
    }
    public void addTwoValueWritable(TwoValueWritable sumCount) {
        set(new IntWritable(this.first.get() + sumCount.getFirst().get()), new IntWritable(this.second.get() + sumCount.getSecond().get()));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }
    @Override
    public int compareTo( TwoValueWritable twoValueWritable){

        int comparison=first.compareTo(twoValueWritable.first);

        if(comparison!=0){
            return comparison;
        }

        return second.compareTo(twoValueWritable.second);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TwoValueWritable twoValueWritable = (TwoValueWritable) o;

        return first.equals(twoValueWritable.first) && second.equals(twoValueWritable.second);
    }

    @Override
    public int hashCode() {
        int result = first.hashCode();
        result = 31 * result + second.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return first + "," + second;
    }
}


