package TopTenLocation;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by ameyutturkar on 4/23/17.
 */
public class TopTen_Mapper extends Mapper<Object, Text, Text, CustomWritableTopTen>
{
    private TreeMap<Text, CustomWritableTopTen> repToRecordMap = new TreeMap<Text, CustomWritableTopTen>();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        Text date = new Text();
        try
        {
            String arr[] = value.toString().split(",");

            if(arr[0].equalsIgnoreCase("medallion") || arr[10] == null || arr[11] == null)
            {
                return;
            }

            String dateArr[] = arr[5].split(" ");
            String dateOnly = dateArr[0];
            String pickupLongitude = arr[10];
            String pickupLatitude = arr[11];
            CustomWritableTopTen customWritableTopTen = new CustomWritableTopTen(pickupLatitude, pickupLongitude);
            repToRecordMap.put(new Text(dateOnly), customWritableTopTen);

            if(repToRecordMap.size() > 10)
            {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        for(Text t : repToRecordMap.keySet())
        {
            context.write(t, repToRecordMap.get(t));
        }
    }
}
