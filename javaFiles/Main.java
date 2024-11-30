package HW4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Main extends Configured implements Tool {

    @Override
    public int run(String[] arguments) throws Exception {
        // Temporary directories for intermediate outputs
        Path temporaryDirectory1 = new Path("data/temp1");
        Path temporaryDirectory2 = new Path("data/temp2");
        Path temporaryDirectory3 = new Path("data/temp3");
        Path outputDirectory = new Path(arguments[1]);

        // Configuration object
        Configuration config = getConf();
        FileSystem fs = FileSystem.get(config);

        // Delete existing temporary directories
        fs.delete(temporaryDirectory1, true);
        fs.delete(temporaryDirectory2, true);
        fs.delete(temporaryDirectory3, true);

        // Delete the output directory if it exists
        fs.delete(outputDirectory, true);

        try {
            // Step 1: Initial MapReduce Job
            Job initialJob = Job.getInstance(config);
            initialJob.setJobName("InitialStep");
            initialJob.setJarByClass(Main.class);

            initialJob.setMapOutputValueClass(Text.class);
            initialJob.setOutputKeyClass(Text.class);
            initialJob.setOutputValueClass(Text.class);

            initialJob.setMapperClass(MapStep1.class);
            initialJob.setReducerClass(ReduceStep1.class);

            FileInputFormat.addInputPath(initialJob, new Path(arguments[0]));
            FileOutputFormat.setOutputPath(initialJob, temporaryDirectory1);

            if (!initialJob.waitForCompletion(true)) {
                return 1;
            }

            // Step 2: Intermediate MapReduce Job
            config = new Configuration();
            Job intermediateJob = Job.getInstance(config);
            intermediateJob.setJobName("IntermediateStep");
            intermediateJob.setJarByClass(Main.class);

            intermediateJob.setMapOutputKeyClass(Text.class);
            intermediateJob.setMapOutputValueClass(Text.class);
            intermediateJob.setOutputKeyClass(Text.class);
            intermediateJob.setOutputValueClass(Text.class);

            intermediateJob.setMapperClass(MapStep2.class);
            intermediateJob.setReducerClass(ReduceStep2.class);

            FileInputFormat.addInputPath(intermediateJob, temporaryDirectory1);
            FileOutputFormat.setOutputPath(intermediateJob, temporaryDirectory2);

            if (!intermediateJob.waitForCompletion(true)) {
                return 1;
            }

            // Step 3: Another Intermediate MapReduce Job
            config = new Configuration();
            Job secondIntermediateJob = Job.getInstance(config);
            secondIntermediateJob.setJobName("SecondIntermediateStep");
            secondIntermediateJob.setJarByClass(Main.class);

            secondIntermediateJob.setMapOutputKeyClass(Text.class);
            secondIntermediateJob.setMapOutputValueClass(IntWritable.class);
            secondIntermediateJob.setOutputKeyClass(Text.class);
            secondIntermediateJob.setOutputValueClass(LongWritable.class);

            secondIntermediateJob.setMapperClass(MapStep3.class);
            secondIntermediateJob.setReducerClass(ReduceStep3.class);

            FileInputFormat.addInputPath(secondIntermediateJob, temporaryDirectory2);
            FileOutputFormat.setOutputPath(secondIntermediateJob, temporaryDirectory3);

            if (!secondIntermediateJob.waitForCompletion(true)) {
                return 1;
            }

            // Step 4: Final MapReduce Job
            config = new Configuration();
            Job finalJob = Job.getInstance(config);
            finalJob.setJobName("FinalStep");
            finalJob.setJarByClass(Main.class);

            finalJob.setMapOutputKeyClass(LongWritable.class);
            finalJob.setMapOutputValueClass(Text.class);
            finalJob.setOutputKeyClass(Text.class);
            finalJob.setOutputValueClass(LongWritable.class);

            finalJob.setMapperClass(MapStep4.class);
            finalJob.setReducerClass(ReduceStep4.class);

            FileInputFormat.addInputPath(finalJob, temporaryDirectory3);
            FileOutputFormat.setOutputPath(finalJob, outputDirectory);

            if (!finalJob.waitForCompletion(true)) {
                return 1;
            }

        } finally {
            // Cleanup temporary directories
            fs.delete(temporaryDirectory1, true);
            fs.delete(temporaryDirectory2, true);
            fs.delete(temporaryDirectory3, true);
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(result);
    }
}
