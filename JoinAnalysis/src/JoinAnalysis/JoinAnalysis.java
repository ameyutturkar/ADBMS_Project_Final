package JoinAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by ameyutturkar on 4/24/17.
 */
public class JoinAnalysis
{
    public static void main(String[] args) throws IOException {
        try
        {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "join analysis");
            job.setJarByClass(JoinAnalysis.class);

            job.setMapperClass(JoinAnalysis_Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(JoinAnalysis_Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
