package JobChaining;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ameyutturkar on 4/25/17.
 */
public class Chaining_Mapper2 extends Mapper<Object, Text, Text, IntWritable>
{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String newArr[] = value.toString().split(",");
        String dateTime = newArr[2];
        String date[] = dateTime.toString().split(" ");
        String dateOnly = date[0];
        String timeOnly = date[1];
        String hour[] = timeOnly.toString().split(":");
        int hourOnly = Integer.parseInt(hour[0]);

        String keyLocation = new StringBuilder().append(newArr[0]).append(",").append(newArr[1]).append(dateOnly).toString();

        context.write(new Text(keyLocation), new IntWritable(hourOnly));
    }
}
