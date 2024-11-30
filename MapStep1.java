package HW4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MapStep1 extends Mapper<LongWritable, Text, Text, Text> {

    // Mapper method
    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context)
            throws IOException, InterruptedException {

        // Convert the input text value to a string
        String lineContent = inputValue.toString();

        // Split the line by commas and extract the SID
        String SID = lineContent.split(",")[1];

        // Remove the SID from the line
        lineContent = lineContent.replaceFirst("," + SID, "");

        // Write the SID and the modified line to the context
        context.write(new Text(SID), new Text(lineContent));
    }
}
