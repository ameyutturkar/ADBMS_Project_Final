package FareAnalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ameyutturkar on 4/22/17.
 */
public class FareAnalysis_Mapper extends Mapper<Object, Text, CompsiteKeyWritable, CustomWritableFA>
{
    Text date = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        try
        {
            String arr[] = value.toString().split(",");

            if(arr[0].equalsIgnoreCase("medallion"))
            {
                return;
            }

            String dateArr[] = arr[3].split(" ");
            String dateOnly = dateArr[0];
            String vendorID = arr[2];
            String paymentType = arr[4];
            Float totalAmount = Float.parseFloat(arr[10]);
            Float fareAmt = Float.parseFloat(arr[5]);
            /*Float surcharge = Float.parseFloat(arr[6]);
            Float tax = Float.parseFloat(arr[7]);
            Float tip = Float.parseFloat(arr[8]);
            Float toll = Float.parseFloat(arr[9]);
            Float minFare = 0.0f;
            Float maxFare = 0.0f;
            String minPT = "";
            String maxPT = "";*/
            CustomWritableFA cfa = new CustomWritableFA(fareAmt, totalAmount);
            CompsiteKeyWritable compsiteKeyWritable = new CompsiteKeyWritable(dateOnly, vendorID, paymentType);
            context.write(compsiteKeyWritable, cfa);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
