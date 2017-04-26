package JobChaining;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ameyutturkar on 4/25/17.
 */
public class Chaining_Reducer1 extends Reducer<CompositeKeyWritable, IntWritable, CompositeKeyWritable, IntWritable>
{
    public void reduce(CompositeKeyWritable key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable val : value)
        {
            count += val.get();
        }
        context.write(key, new IntWritable(count));
    }
}
