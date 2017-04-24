package TopTenLocation;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ameyutturkar on 4/23/17.
 */
public class CustomWritableTopTen implements Writable
{
    private String longitude;
    private String latitude;

    public CustomWritableTopTen()
    {

    }

    public CustomWritableTopTen(String longitude, String latitude)
    {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, longitude);
        WritableUtils.writeString(dataOutput, latitude);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        longitude = WritableUtils.readString(dataInput);
        latitude = WritableUtils.readString(dataInput);
    }

    public String toString()
    {
        return (new StringBuilder().append(longitude).append(",").append(latitude).toString());
    }
}
