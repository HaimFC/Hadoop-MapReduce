package HW4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

public class ReduceStep2 extends Reducer<Text, Text, Text, Text> {

    // Reducer method
    @Override
    public void reduce(Text inputKey, Iterable<Text> inputValues, Context context)
            throws IOException, InterruptedException {

        for (Text value : inputValues) {
            // Convert the value to a string and split by comma and space
            String lineContent = value.toString();
            String[] lineParts = lineContent.split(", ");

            if (lineParts.length < 3) {
                // Extract parts based on the length of lineParts
                String part1 = lineParts[0].split(",", 2)[1];
                String part2;
                if (lineParts.length == 2) {
                    part2 = lineParts[1].split(",", 2)[1];
                } else {
                    part2 = String.join(",", part1.split(",")[0], "0");
                }
                String part3 = String.join(",", part2.split(",")[0], "0");
                context.write(inputKey, new Text(String.join(" ", part1, part2, part3)));
            }

            // Iterate through lineParts to create combinations of parts
            for (int i = 0; i <= lineParts.length - 3; i++) {
                String part1 = lineParts[i].split(",", 2)[1];
                String part2 = lineParts[i + 1].split(",", 2)[1];
                String part3 = lineParts[i + 2].split(",", 2)[1];
                context.write(inputKey, new Text(String.join(" ", part1, part2, part3)));
            }
        }
    }
}
