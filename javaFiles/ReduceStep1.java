package HW4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

public class ReduceStep1 extends Reducer<Text, Text, Text, Text> {

    // Reducer method
    @Override
    public void reduce(Text inputKey, Iterable<Text> inputValues, Context context)
            throws IOException, InterruptedException {

        // TreeSet to store unique values in sorted order as hinted in the PDF
        TreeSet<String> uniqueValuesSet = new TreeSet<String>();
        for (Text value : inputValues) {
            uniqueValuesSet.add(value.toString());
        }

        // Write the key and the sorted unique values to context
        context.write(inputKey, new Text(uniqueValuesSet.toString()));
    }
}
