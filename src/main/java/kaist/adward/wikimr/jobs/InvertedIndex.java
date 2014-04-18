package kaist.adward.wikimr.jobs;

import kaist.adward.wikimr.mappers.InvertedIndexMapper;
import kaist.adward.wikimr.reducers.InvertedIndexReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex {

    public InvertedIndex() {
        System.out.println("Init InvertedIndex");
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // strip off generic options and take command options, which are input and output file paths
        String[] commandArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (commandArgs.length != 3) {
            System.err.println("Usage: InvertedIndex <input path> <output path> <distributed cache file path>");
            System.exit(-1);
        }

        Job job = new Job(conf, "Inverted Index");

        //job.setCombinerClass(InvertedIndexReducer.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setJarByClass(InvertedIndex.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(commandArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(commandArgs[1]));

        // add distributed cache
        job.addCacheFile(new Path(commandArgs[2]).toUri());

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}