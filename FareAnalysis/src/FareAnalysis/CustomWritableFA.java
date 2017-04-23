package FareAnalysis;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ameyutturkar on 4/22/17.
 */
public class CustomWritableFA implements Writable
{
    private float fareAmount;
    private float totalAmt;
    private float minAmt;
    private float maxAmt;
    private float avg;

    public CustomWritableFA()
    {

    }

    public CustomWritableFA(float fareAmount, float totalAmt)
    {
        this.fareAmount = fareAmount;
        this.totalAmt = totalAmt;
    }

    public CustomWritableFA(float fareAmount, float minAmt, float maxAmt, float avg, float totalAmt)
    {
        this.fareAmount = fareAmount;
        this.minAmt = minAmt;
        this.maxAmt = maxAmt;
        this.avg = avg;
        this.totalAmt = totalAmt;
    }

    public float getFareAmount() {
        return fareAmount;
    }

    public void setFareAmount(float fareAmount) {
        this.fareAmount = fareAmount;
    }

    public float getTotalAmt() {
        return totalAmt;
    }

    public void setTotalAmt(float totalAmt) {
        this.totalAmt = totalAmt;
    }

    public float getMinAmt() {
        return minAmt;
    }

    public void setMinAmt(float minAmt) {
        this.minAmt = minAmt;
    }

    public float getMaxAmt() {
        return maxAmt;
    }

    public void setMaxAmt(float maxAmt) {
        this.maxAmt = maxAmt;
    }

    public float getAvg() {
        return avg;
    }

    public void setAvg(float avg) {
        this.avg = avg;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        WritableUtils.writeString(dataOutput, Float.toString(fareAmount));
        WritableUtils.writeString(dataOutput, Float.toString(totalAmt));
        WritableUtils.writeString(dataOutput, Float.toString(minAmt));
        WritableUtils.writeString(dataOutput, Float.toString(maxAmt));
        WritableUtils.writeString(dataOutput, Float.toString(avg));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        fareAmount = Float.parseFloat(WritableUtils.readString(dataInput));
        totalAmt = Float.parseFloat(WritableUtils.readString(dataInput));
        minAmt = Float.parseFloat(WritableUtils.readString(dataInput));
        maxAmt = Float.parseFloat(WritableUtils.readString(dataInput));
        avg = Float.parseFloat(WritableUtils.readString(dataInput));
    }


    public String toString()
    {
        return (new StringBuilder().append(fareAmount).append(",").append(minAmt).append(",").append(maxAmt).append(",").append(avg).append(",").append(totalAmt).toString());
    }
}
