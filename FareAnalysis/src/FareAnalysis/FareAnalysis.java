package FareAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by ameyutturkar on 4/22/17.
 */
public class FareAnalysis
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        try
        {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Fare Analysis");
            job.setJarByClass(FareAnalysis.class);
            job.setMapperClass(FareAnalysis_Mapper.class);
            job.setMapOutputKeyClass(CompsiteKeyWritable.class);
            job.setMapOutputValueClass(CustomWritableFA.class);

            job.setReducerClass(FareAnalysis_Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(CustomWritableFA.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 :1);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
