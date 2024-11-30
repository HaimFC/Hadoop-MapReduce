package HW4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MapStep2 extends Mapper<LongWritable, Text, Text, Text> {

    // Mapper method
    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context)
            throws IOException, InterruptedException {

        // Convert the input text value to a string
        String lineContent = inputValue.toString();

        // Extract the part of the string before the first '['
        String prefixString = lineContent.split("\\[")[0];

        // Remove the extracted part and '[' and ']' characters from the line
        lineContent = lineContent.replaceFirst(prefixString + "\\[", "").replaceAll("\\]", "");

        // Extract the SID
        String SID = prefixString.split("\\t")[0];

        // Write the SID and the modified line to the context
        context.write(new Text(SID), new Text(lineContent));
    }
}
