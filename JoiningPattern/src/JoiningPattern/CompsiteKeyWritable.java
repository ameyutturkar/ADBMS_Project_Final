package JoiningPattern;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ameyutturkar on 4/23/17.
 */
public class CompsiteKeyWritable implements Writable, WritableComparable<CompsiteKeyWritable>
{
    private String inputDateTime;
    private String vendorID;

    public CompsiteKeyWritable()
    {

    }

    public CompsiteKeyWritable(String inputDateTime, String vendorID)
    {
        this.inputDateTime = inputDateTime;
        this.vendorID = vendorID;
    }

    public String getInputDateTime() {
        return inputDateTime;
    }

    public void setInputDateTime(String inputDateTime) {
        this.inputDateTime = inputDateTime;
    }

    public String getVendorID() {
        return vendorID;
    }

    public void setVendorID(String vendorID) {
        this.vendorID = vendorID;
    }

    @Override
    public void write(DataOutput d) throws IOException
    {
        WritableUtils.writeString(d, inputDateTime);
        WritableUtils.writeString(d, vendorID);
    }

    @Override
    public void readFields(DataInput di) throws IOException
    {
        inputDateTime = WritableUtils.readString(di);
        vendorID = WritableUtils.readString(di);
    }

    @Override
    public int compareTo(CompsiteKeyWritable ckw)
    {
        int result = inputDateTime.compareTo(ckw.inputDateTime);
        if(result == 0)
        {
            result = vendorID.compareTo(ckw.vendorID);
        }
        return result;
    }

    public String toString()
    {
        return (new StringBuilder().append(inputDateTime).append(",").append(vendorID).toString());
    }
}
