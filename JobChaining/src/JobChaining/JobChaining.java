package JobChaining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by ameyutturkar on 4/25/17.
 */
public class JobChaining
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "chaining");
        job1.setJarByClass(JobChaining.class);

        job1.setMapperClass(Chaining_Mapper1.class);
        job1.setMapOutputKeyClass(CompositeKeyWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setReducerClass(Chaining_Reducer1.class);
        job1.setOutputKeyClass(CompositeKeyWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        boolean complete = job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "chaining");

        if(complete)
        {
            job2.setJarByClass(JobChaining.class);
            job2.setMapperClass(Chaining_Mapper2.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(IntWritable.class);

            job2.setReducerClass(Chaining_Reducer2.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
