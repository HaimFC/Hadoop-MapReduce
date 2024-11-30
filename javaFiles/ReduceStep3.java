package HW4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceStep3 extends Reducer<Text, IntWritable, Text, LongWritable> {

    // Reducer method
    @Override
    public void reduce(Text inputKey, Iterable<IntWritable> inputValues, Context context)
            throws IOException, InterruptedException {

        // Initialize a counter to zero
        long counter = 0;
        for (IntWritable value : inputValues) {
            counter++; // Increment the counter for each value
        }

        // Write the key and the count to the context
        context.write(inputKey, new LongWritable(counter));
    }
}
