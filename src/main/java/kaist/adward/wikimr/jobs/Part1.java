package kaist.adward.wikimr.jobs;

import kaist.adward.wikimr.mappers.InvertedIndexMapper;
import kaist.adward.wikimr.mappers.TopNMapper;
import kaist.adward.wikimr.reducers.InvertedIndexReducer;
import kaist.adward.wikimr.reducers.TopNReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by adward on 4/7/14.
 */
public class Part1 {
	public static void main(String[] args) throws Exception {
		// Job 1: inverted index
		Configuration invIdxConf = new Configuration();

		// strip off generic options and take command options, which are input and output file paths
		String[] commandArgs = new GenericOptionsParser(invIdxConf, args).getRemainingArgs();
		if (commandArgs.length != 3) {
			System.err.println("Usage: Part1 <input path> <working path> <N>");
			System.exit(-1);
		}

		Job invIdxJob = Job.getInstance(invIdxConf, "Inverted Index");

		//job.setCombinerClass(InvertedIndexReducer.class);
		invIdxJob.setMapperClass(InvertedIndexMapper.class);
		invIdxJob.setReducerClass(InvertedIndexReducer.class);

		invIdxJob.setJarByClass(InvertedIndex.class);
		invIdxJob.setOutputKeyClass(Text.class);
		invIdxJob.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(invIdxJob, new Path(commandArgs[0]));
		FileOutputFormat.setOutputPath(invIdxJob, new Path(commandArgs[1] + "/temp"));

		// add distributed cache
//		invIdxJob.addCacheFile(new Path(commandArgs[2]).toUri());

		if (!invIdxJob.waitForCompletion(true))
			System.exit(1);

		// Job 2: top 200
		Configuration topNConf = new Configuration();

		// pass parameter N to configuration
		// so that it can be used by mappers and reducers
		topNConf.set("N", commandArgs[2]);

		Job topNJob = Job.getInstance(topNConf, "Top " + commandArgs[2]);

		//job.setCombinerClass(TopNReducer.class);
		topNJob.setMapperClass(TopNMapper.class);
		topNJob.setReducerClass(TopNReducer.class);

		topNJob.setJarByClass(TopN.class);
		topNJob.setOutputKeyClass(NullWritable.class);
		topNJob.setOutputValueClass(Text.class);

		// for this specific job, we must have single reducer to finalize top N
		topNJob.setNumReduceTasks(1);

		FileInputFormat.addInputPath(topNJob, new Path(commandArgs[1] + "/temp"));
		FileOutputFormat.setOutputPath(topNJob, new Path(commandArgs[1] + "/output"));

		System.exit(topNJob.waitForCompletion(true) ? 0 : 1);
	}
}
