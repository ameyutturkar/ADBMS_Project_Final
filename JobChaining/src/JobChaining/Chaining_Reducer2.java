package JobChaining;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ameyutturkar on 4/25/17.
 */
public class Chaining_Reducer2 extends Reducer<Text, IntWritable, Text, Text>
{
    public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
        int dayRide = 0;
        int nightRide = 0;
        for(IntWritable val : value)
        {
            if(val.get() >= 8 && val.get() <= 20)
            {
                dayRide += 1;
            }
            else
            {
                nightRide += 1;
            }
        }
        String outValue = new StringBuilder().append("Day Time Rides: ").append(dayRide).append(" | ").append("Night Time Rides: ").append(nightRide).append(" | ").toString();
        context.write(key, new Text(outValue));
    }
}
