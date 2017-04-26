package JobChaining;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by ameyutturkar on 4/23/17.
 */
public class GroupComparator extends WritableComparator
{
    protected GroupComparator()
    {
        super(CompositeKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2)
    {
        CompositeKeyWritable cw1 = (CompositeKeyWritable) w1;
        CompositeKeyWritable cw2 = (CompositeKeyWritable) w2;

        return (cw1.getPickupLongitude().compareTo(cw2.getPickupLongitude()));
    }
}
