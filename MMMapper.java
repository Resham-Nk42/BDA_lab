import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class MeanMaxMapper extends Mapper < LongWritable, Text, Text, IntWritable > {
    public static final int MISSING = 9999;
    @Override
    public void map(LongWritable key, Text value, Context context)
    throws IOException,
    InterruptedException {
        String line = value.toString();
        // Extract month from positions 19-20
        String month = line.substring(19, 21);
        int temperature;
        // Extract temperature considering optional '+'
        if (line.charAt(87) == '+') {
            temperature = Integer.parseInt(line.substring(88, 92));
        } else {
            temperature = Integer.parseInt(line.substring(87, 92));
        }
        // Quality check
        String quality = line.substring(92, 93);
        if (temperature != MISSING && quality.matches("[01459]")) {
            context.write(new Text(month), new IntWritable(temperature));
        }
    }
}
