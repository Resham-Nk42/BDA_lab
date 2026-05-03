import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class MeanMaxReducer extends Reducer < Text, IntWritable, Text, Text > {
    @Override
    public void reduce(Text key, Iterable < IntWritable > values,
        Context context) throws IOException,
    InterruptedException {
        int sumTemp = 0;
        int count = 0;
        int maxTemp = Integer.MIN_VALUE;
        for (IntWritable value: values) {
            int temp = value.get();
            sumTemp += temp;
            count++;
            if (temp > maxTemp) {
                maxTemp = temp;
            }
        }
        if (count > 0) {
            int avgTemp = sumTemp / count;
            String result = "mean=" + avgTemp + " max=" + maxTemp;
            context.write(key, new Text(result));
        }
    }
}
