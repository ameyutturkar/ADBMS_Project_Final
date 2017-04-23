package FareAnalysis;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

/**
 * Created by ameyutturkar on 4/23/17.
 */
public class CompsiteKeyWritable implements Writable, WritableComparable<CompsiteKeyWritable>
{
    private String inputDate;
    private String vendorID;
    private String paymentType;

    public CompsiteKeyWritable()
    {

    }

    public CompsiteKeyWritable(String inputDate, String vendorID, String paymentType)
    {
        this.inputDate = inputDate;
        this.vendorID = vendorID;
        this.paymentType = paymentType;
    }

    public String getInputDate() {
        return inputDate;
    }

    public void setInputDate(String inputDate) {
        this.inputDate = inputDate;
    }

    public String getVendorID() {
        return vendorID;
    }

    public void setVendorID(String vendorID) {
        this.vendorID = vendorID;
    }

    public String getPaymentType() {
        return paymentType;
    }

    public void setPaymentType(String paymentType) {
        this.paymentType = paymentType;
    }

    @Override
    public void write(DataOutput d) throws IOException
    {
        WritableUtils.writeString(d, inputDate);
        WritableUtils.writeString(d, vendorID);
        WritableUtils.writeString(d, paymentType);
    }

    @Override
    public void readFields(DataInput di) throws IOException
    {
        inputDate = WritableUtils.readString(di);
        vendorID = WritableUtils.readString(di);
        paymentType = WritableUtils.readString(di);
    }

    @Override
    public int compareTo(CompsiteKeyWritable ckw)
    {
        int result = inputDate.compareTo(ckw.inputDate);
        if(result == 0)
        {
            result = vendorID.compareTo(ckw.vendorID);
            if(result == 0)
            {
                result = paymentType.compareTo(ckw.paymentType);
            }
        }
        return result;
    }

    public String toString()
    {
        return (new StringBuilder().append(inputDate).append(",").append(vendorID).append(",").append(paymentType).toString());
    }
}
