package FareAnalysis;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by ameyutturkar on 4/23/17.
 */
public class GroupComparator extends WritableComparator
{
    protected GroupComparator()
    {
        super(CompsiteKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2)
    {
        CompsiteKeyWritable cw1 = (CompsiteKeyWritable) w1;
        CompsiteKeyWritable cw2 = (CompsiteKeyWritable) w2;

        return (cw1.getInputDate().compareTo(cw2.getInputDate()));
    }
}
