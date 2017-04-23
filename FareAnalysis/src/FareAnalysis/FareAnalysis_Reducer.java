package FareAnalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ameyutturkar on 4/22/17.
 */
public class FareAnalysis_Reducer extends Reducer<CompsiteKeyWritable, CustomWritableFA, CompsiteKeyWritable, CustomWritableFA>
{
    public void reduce(CompsiteKeyWritable key, Iterable<CustomWritableFA> value, Context context) throws IOException, InterruptedException
    {
        float fareAmount = 0;
        float totalAmt = 0;
        int count = 0;
        float avg = 0;
        float maxFare = 0;
        float minFare = 500;

        for(CustomWritableFA val : value)
        {
            System.out.print(val.getFareAmount());
            if(val.getTotalAmt() >= maxFare)
            {
                maxFare = val.getTotalAmt();
            }
            else if(val.getTotalAmt() <= minFare)
            {
                minFare = val.getTotalAmt();
            }
            totalAmt += val.getTotalAmt();
            fareAmount += val.getFareAmount();
            count += 1;
        }
        avg = fareAmount/count;

        context.write(key, new CustomWritableFA(fareAmount, minFare, maxFare, avg, totalAmt));
    }
}
