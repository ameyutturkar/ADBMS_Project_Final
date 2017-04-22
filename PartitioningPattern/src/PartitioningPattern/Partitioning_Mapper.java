package PartitioningPattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ameyutturkar on 4/22/17.
 */
public class Partitioning_Mapper extends Mapper<Object, Text, Text, Text>
{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Text date = new Text();
        Text dateOnly = new Text();

        try
        {
            String row[] = value.toString().split(",");

            if(row[0].equalsIgnoreCase("medallion"))
            {
                return;
            }
            String dateArr[] = row[3].split("-");
            String dateArr1[] = dateArr[2].split(" ");
            date.set(dateArr1[0]);
            context.write(date, value);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
