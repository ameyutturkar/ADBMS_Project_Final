package JoinAnalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by ameyutturkar on 4/24/17.
 */
public class JoinAnalysis_Mapper extends Mapper<Object, Text, Text, Text>
{
    public void map(Object key, Text value, Context context)
    {
        try
        {
            String arr[] = value.toString().split(",");

            if(arr[0].equalsIgnoreCase("medallion"))
            {
                return;
            }

            if(arr[3].contains("M1"))
            {
                String paymentType = arr[4];
                String fareAmount = arr[5];
                String surcharge = arr[6];
                String tax = arr[7];
                String toll = arr[8];
                String totalAmount = arr[9];

                context.write(key, );
            }
            else
            {
                String tripTime = arr[4];
                String tripDistance = arr[5];
                String pickupLongitude = arr[6];
                String pickupLatitude = arr[7];
                String dropLongitude = arr[8];
                String dropLatitude = arr[9];
            }


        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
