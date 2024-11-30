package HW4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.TreeSet;

public class ReduceStep4 extends Reducer<LongWritable, Text, Text, LongWritable> {

    // Reducer method
    @Override
    public void reduce(LongWritable inputKey, Iterable<Text> inputValues, Context context)
            throws IOException, InterruptedException {

        // TreeSet to store unique values in sorted order
        TreeSet<String> uniqueValuesSet = new TreeSet<String>();
        for (Text value : inputValues) {
            uniqueValuesSet.add(value.toString());
        }

        // Iterate through the sorted unique values
        for (String value : uniqueValuesSet) {
            // Get the negated count from the key
            int counter = -Integer.parseInt(inputKey.toString());

            // Write the extracted part and the negated count to context
            context.write(new Text(value.split("\\*")[1]), new LongWritable(counter));
        }
    }
}
