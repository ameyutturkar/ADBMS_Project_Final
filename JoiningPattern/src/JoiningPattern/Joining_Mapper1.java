package JoiningPattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ameyutturkar on 4/24/17.
 */
public class Joining_Mapper1 extends Mapper<Object, Text, CompsiteKeyWritable, Text>
{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        try
        {
            String arr[] = value.toString().split(",");

            if(arr[0].equalsIgnoreCase("medallion"))
            {
                return;
            }

            String medallion = arr[0];
            String hackLicense = arr[1];
            String pickupDateTime = arr[3];
            String vendorID = arr[2];
            String paymentType = arr[4];
            String fareAmount = arr[5];
            String surcharge = arr[6];
            String tax = arr[7];
            String toll = arr[8];
            String totalAmount = arr[10];
            String outValue = new StringBuilder().append("M1").append(",").append(paymentType).append(",").append(fareAmount).append(",").append(surcharge).append(",").append(tax).append(",").append(toll).append(",").append(totalAmount).toString();
            CompsiteKeyWritable ckw = new CompsiteKeyWritable(medallion, hackLicense, vendorID, pickupDateTime);
            context.write(ckw, new Text(outValue));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
