package JobChaining;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ameyutturkar on 4/25/17.
 */
public class Chaining_Mapper1 extends Mapper<Object, Text, CompositeKeyWritable, IntWritable>
{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        try
        {
            IntWritable one = new IntWritable(1);
            String arr[] = value.toString().split(",");

            if(arr[0].equalsIgnoreCase("medallion"))
            {
                return;
            }

            String pickupLongitude = arr[10];
            String pickupLatitude = arr[11];
            String pickupDateTime = arr[5];

            CompositeKeyWritable ckw = new CompositeKeyWritable(pickupLongitude, pickupLatitude, pickupDateTime);
            context.write(ckw, one);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
