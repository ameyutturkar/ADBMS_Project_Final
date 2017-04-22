package PartitioningPattern;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by ameyutturkar on 4/22/17.
 */
public class Partitioning_Partitioner extends Partitioner<Text, Text> implements Configurable
{
    private Configuration conf = null;

    @Override
    public int getPartition(Text key, Text value, int i)
    {
        String str = key.toString();
        int date = 0;

        switch (str)
        {
            case "01":
                date = 0;
                break;

            case "02":
                date = 1;
                break;

            case "03":
                date = 2;
                break;

            case "04":
                date = 3;
                break;

            case "05":
                date = 4;
                break;

            case "06":
                date = 5;
                break;

            case "07":
                date = 6;
                break;

            case "08":
                date = 7;
                break;

            case "09":
                date = 8;
                break;

            case "10":
                date = 9;
                break;

            case "11":
                date = 10;
                break;

            case "12":
                date = 11;
                break;

            case "13":
                date = 12;
                break;

            case "14":
                date = 13;
                break;

            case "15":
                date = 14;
                break;

            case "16":
                date = 15;
                break;

            case "17":
                date = 16;
                break;

            case "18":
                date = 17;
                break;

            case "19":
                date = 18;
                break;

            case "20":
                date = 19;
                break;

            case "21":
                date = 20;
                break;

            case "22":
                date = 21;
                break;

            case "23":
                date = 22;
                break;

            case "24":
                date = 23;
                break;

            case "25":
                date = 24;
                break;

            case "26":
                date = 25;
                break;

            case "27":
                date = 26;
                break;

            case "28":
                date = 27;
                break;

            case "29":
                date = 28;
                break;

            case "30":
                date = 29;
                break;

            case "31":
                date = 30;
                break;

                default: date = 999;
                break;
        }
        return date;
    }
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
