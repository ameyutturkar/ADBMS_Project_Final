package JoiningPattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ameyutturkar on 4/24/17.
 */
public class Joining_Mapper2 extends Mapper<Object, Text, CompsiteKeyWritable, Text>
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

            String pickupDateTime = arr[5];
            String vendorID = arr[2];
            String tripTime = arr[8];
            String tripDistance = arr[9];
            String pickupLongitude = arr[10];
            String pickupLatitude = arr[11];
            String dropLongitude = arr[12];
            String dropLatitude = arr[13];

            String outValue = new StringBuilder().append("M2").append(",").append(tripTime).append(",").append(tripDistance).append(",").append(pickupLongitude).append(",").append(pickupLatitude).append(",").append(dropLongitude).append(",").append(dropLatitude).toString();
            CompsiteKeyWritable ckw = new CompsiteKeyWritable(vendorID, pickupDateTime);
            context.write(ckw, new Text(outValue));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
