import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UVDriver extends Configured implements Tool {

	public static class MPreMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Text keyText = new Text();
		private Text valText = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Input: (lineNo, lineContent)
			
			// Split each line using seperator based on the dataset.
			String line[] = null;
			String seperator = null;
			if (Constants.BIG_DATA) {
				seperator = Constants.BIG_DATA_SEPERATOR;
			} else {
				seperator = Constants.SMALL_DATA_SEPERATOR;
			}

			line = value.toString().split(seperator);

			keyText.set(line[0]);
			valText.set(line[1] + "," + line[2]);

			// Output: (userid, "movieid,rating")
			output.collect(keyText, valText);
		}
	}

	public static class MPreReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		private Text valText = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Input: (userid, List<movieid, rating>)

			float sum = 0.0F;
			int totalRatingCount = 0;

			ArrayList<String> movieID = new ArrayList<String>();
			ArrayList<Float> rating = new ArrayList<Float>();

			while (values.hasNext()) {
				String[] movieRatingPair = values.next().toString().split(",");
				movieID.add(movieRatingPair[0]);
				Float parseRating = Float.parseFloat(movieRatingPair[1]);
				rating.add(parseRating);

				sum += parseRating;
				totalRatingCount++;
			}

			float average = ((float) sum) / totalRatingCount;

			for (int i = 0; i < movieID.size(); i++) {
				valText.set(key.toString() + " " + movieID.get(i) + " "
						+ (rating.get(i) - average));
				output.collect(null, valText);
			}
			
			// Output: (null, <userid, movieid, normalizedrating>)
			
		}
	}
	
	
	
	public static class Constants {
		public static final boolean BIG_DATA = false;
		public static final String BIG_DATA_SEPERATOR = "::";
		public static final String SMALL_DATA_SEPERATOR = "\\s";
	}

	public static class JobRunner implements Runnable {
		private JobControl control;

		public JobRunner(JobControl _control) {
			this.control = _control;
		}

		public void run() {
			this.control.run();
		}
	}

	public static void handleRun(JobControl control)
			throws InterruptedException {
		JobRunner runner = new JobRunner(control);
		Thread t = new Thread(runner);
		t.start();

		while (!control.allFinished()) {
			System.out.println(new Date().toString() + ": Still running...");
			System.out.println("Waiting jobs: " + control.getWaitingJobs().toString());
			System.out.println("Successful jobs: " + control.getSuccessfulJobs().toString());
			Thread.sleep(5000);
		}
	}

	public int run(String[] args) throws Exception {

		// 1. Pre-process the data.
		// a) Normalize
		// 2. Initialize the U, V Matrices
		// a) Initialize U Matrix
		// b) Initialize V Matrix
		// 3. Iterate to update U and V.

		// Write Job details for each of the above steps.
		
		// 1. Pre-process the data.

		JobConf conf1 = new JobConf(UVDriver.class);
		conf1.setMapperClass(MPreMap.class);
		conf1.setReducerClass(MPreReduce.class);
		conf1.setJarByClass(UVDriver.class);

		conf1.setMapOutputKeyClass(Text.class);
		conf1.setMapOutputValueClass(Text.class);

		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(conf1, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf1, new Path(args[1]));

		// Run the jobs.
		Job job1 = new Job(conf1);

		JobControl jobControl = new JobControl("jobControl");
		jobControl.addJob(job1);

		handleRun(jobControl);
		return 0;
	}

	public static void main(String args[]) throws Exception {

		System.out.println("Program started");
		if (args.length != 2) {
			System.err
					.println("Usage: UVDriver <input path> <output path>");
			System.exit(-1);
		}

		Configuration configuration = new Configuration();
		String[] otherArgs = new GenericOptionsParser(configuration, args)
				.getRemainingArgs();
		ToolRunner.run(new UVDriver(), otherArgs);
		System.out.println("Program complete.");
		System.exit(0);
	}

}
