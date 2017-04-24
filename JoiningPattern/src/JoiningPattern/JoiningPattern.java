package JoiningPattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by ameyutturkar on 4/24/17.
 */
public class JoiningPattern
{
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "join pattern");
        job.setJarByClass(JoiningPattern.class);

        job.setMapOutputKeyClass(CompsiteKeyWritable.class);
        job.setMapOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Joining_Mapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Joining_Mapper2.class);
        job.getConfiguration().set("join.type", "innerJoin");
        job.setReducerClass(Joining_Reducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}
