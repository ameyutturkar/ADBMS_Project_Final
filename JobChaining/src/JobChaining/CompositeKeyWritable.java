package JobChaining;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ameyutturkar on 4/25/17.
 */
public class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable>
{
    private String pickupLongitude;
    private String pickupLatitude;
    private String pickupDateTime;

    public CompositeKeyWritable()
    {

    }

    public CompositeKeyWritable(String pickupLongitude, String pickupLatitude, String pickupDateTime)
    {
        this.pickupLongitude = pickupLongitude;
        this.pickupLatitude = pickupLatitude;
        this.pickupDateTime = pickupDateTime;
    }

    public String getPickupLongitude() {
        return pickupLongitude;
    }

    public void setPickupLongitude(String pickupLongitude) {
        this.pickupLongitude = pickupLongitude;
    }

    public String getPickupLatitude() {
        return pickupLatitude;
    }

    public void setPickupLatitude(String pickupLatitude) {
        this.pickupLatitude = pickupLatitude;
    }

    public String getPickupDateTime() {
        return pickupDateTime;
    }

    public void setPickupDateTime(String pickupDateTime) {
        this.pickupDateTime = pickupDateTime;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        WritableUtils.writeString(d, pickupLongitude);
        WritableUtils.writeString(d, pickupLatitude);
        WritableUtils.writeString(d, pickupDateTime);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        pickupLongitude = WritableUtils.readString(di);
        pickupLatitude = WritableUtils.readString(di);
        pickupDateTime = WritableUtils.readString(di);
    }

    @Override
    public int compareTo(CompositeKeyWritable ckw)
    {
        int result = pickupLongitude.compareTo(ckw.pickupLongitude);
        if(result == 0)
        {
            result = pickupLatitude.compareTo(ckw.pickupLatitude);
            if(result == 0)
            {
                result = pickupDateTime.compareTo(ckw.pickupDateTime);
            }
        }
        return result;
    }

    @Override
    public String toString()
    {
        return (new StringBuilder().append(pickupLongitude).append(",").append(pickupLatitude).append(",").append(pickupDateTime).toString());
    }
}
