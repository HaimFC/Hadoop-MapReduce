package HW4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MapStep3 extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Mapper method
    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context)
            throws IOException, InterruptedException {

        // Convert the input text value to a string
        String lineContent = inputValue.toString();

        // Extract the part of the string before the first tab
        String prefixString = lineContent.split("\\t")[0];

        // Remove the extracted part and the tab character from the line
        lineContent = lineContent.replaceFirst(prefixString + "\\t", "");

        // Get the last character of the line
        String lastCharacter = lineContent.substring(lineContent.length() - 1);

        // Check if the last character is '1'
        if (lastCharacter.equals("1")) {
            // Remove the last character if it's '1'
            lineContent = lineContent.substring(0, lineContent.lastIndexOf(" "));

            // Write the modified line and count 1 to context
            context.write(new Text(lineContent), new IntWritable(1));
        }
    }
}
