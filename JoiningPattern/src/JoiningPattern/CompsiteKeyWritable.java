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
    private String medallion;
    private String hackLicense;

    public CompsiteKeyWritable()
    {

    }

    public CompsiteKeyWritable(String medallion, String hackLicense, String inputDateTime, String vendorID)
    {
        this.inputDateTime = inputDateTime;
        this.vendorID = vendorID;
        this.medallion = medallion;
        this.hackLicense = hackLicense;
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

    public String getMedallion() {
        return medallion;
    }

    public void setMedallion(String medallion) {
        this.medallion = medallion;
    }

    public String getHackLicense() {
        return hackLicense;
    }

    public void setHackLicense(String hackLicense) {
        this.hackLicense = hackLicense;
    }

    @Override
    public void write(DataOutput d) throws IOException
    {
        WritableUtils.writeString(d, inputDateTime);
        WritableUtils.writeString(d, vendorID);
        WritableUtils.writeString(d, medallion);
        WritableUtils.writeString(d, hackLicense);
    }

    @Override
    public void readFields(DataInput di) throws IOException
    {
        inputDateTime = WritableUtils.readString(di);
        vendorID = WritableUtils.readString(di);
        medallion = WritableUtils.readString(di);
        hackLicense = WritableUtils.readString(di);
    }

    @Override
    public int compareTo(CompsiteKeyWritable ckw)
    {
        int result = inputDateTime.compareTo(ckw.inputDateTime);
        if(result == 0)
        {
            result = vendorID.compareTo(ckw.vendorID);
            if(result == 0)
            {
                result = medallion.compareTo(ckw.medallion);
                if(result == 0)
                {
                    result = hackLicense.compareTo(ckw.hackLicense);
                }
            }
        }
        return result;
    }

    public String toString()
    {
        return (new StringBuilder().append(medallion).append(",").append(hackLicense).append(",").append(inputDateTime).append(",").append(vendorID).toString());
    }
}
