package HW4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapStep4 extends Mapper<LongWritable, Text, LongWritable, Text> {

    // Mapper method
    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context)
            throws IOException, InterruptedException {

        // Convert the input text value to a string
        String lineContent = inputValue.toString();

        // Get the count and negate it
        int counter = -Integer.parseInt(lineContent.split("\\t")[1]);

        // Extract the word part of the line
        String wordPart = lineContent.split("\\t")[0];
        String[] splitsArray = wordPart.split(" ");  // Split the word by spaces

        // Extract the first and second parts before the comma
        splitsArray[0] = splitsArray[0].split(",")[0];
        splitsArray[1] = splitsArray[1].split(",")[0];

        // Join the extracted parts with a comma
        String joinedParts = String.join(",", splitsArray[0], splitsArray[1]);

        // Join the result with the original word separated by '*'
        String finalLineContent = String.join("*", joinedParts, wordPart);

        // Write the negated count and the modified line to context
        context.write(new LongWritable(counter), new Text(finalLineContent));
    }
}
