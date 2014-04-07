import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

	public static long starttime;
	public static long endtime;
	public static String uMatrixPath = "U_0";
	public static String vMatrixPath = "V_0";

	public static void startTimer() {
		starttime = System.currentTimeMillis();
	}

	public static void stopTimer() {
		endtime = System.currentTimeMillis();
	}

	public static float getJobTimeInSecs() {
		return (endtime - starttime) / (float) 1000;
	}

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
			if (Settings.BIG_DATA) {
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
				valText.set("M " + key.toString() + " " + movieID.get(i) + " "
						+ (rating.get(i) - average));
				output.collect(null, valText);
			}

			// Output: (null, <M userid, movieid, normalizedrating>)

		}
	}

	public static class UpdateUMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable arg0, Text arg1,
				OutputCollector<Text, Text> arg2, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub

		}

	}

	public static class UpdateUReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text arg0, Iterator<Text> arg1,
				OutputCollector<Text, Text> arg2, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub

		}

	}

	public static class UpdateVMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable arg0, Text arg1,
				OutputCollector<Text, Text> arg2, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub

		}

	}

	public static class UpdateVReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text arg0, Iterator<Text> arg1,
				OutputCollector<Text, Text> arg2, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub

		}

	}

	public void optimizeUVMatrix(int matrixType, int iteration)
			throws IOException, InterruptedException {
		JobConf conf2 = new JobConf(UVDriver.class);

		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(Text.class);

		// Set the paths of the previous iteration matrices
		// so that we can load them when needed during job.
		conf2.set("uPath", Settings.TEMP_PATH + "/" + uMatrixPath);
		conf2.set("vPath", Settings.TEMP_PATH + "/" + vMatrixPath);

		Path matricesPaths[] = new Path[2];

		if (matrixType == Constants.U_Matrix) {
			conf2.setMapperClass(UpdateUMapper.class);
			conf2.setReducerClass(UpdateUReducer.class);
			// Setting the path of the previous iteration matrix.
			matricesPaths[0] = new Path(Settings.TEMP_PATH + "/" + uMatrixPath);
			// Updating path of the current iteration matrix.
			uMatrixPath = "U_" + iteration;

		} else if (matrixType == Constants.V_Matrix) {
			conf2.setMapperClass(UpdateVMapper.class);
			conf2.setReducerClass(UpdateVReducer.class);
			// Setting the path of the previous iteration matrix.
			matricesPaths[0] = new Path(Settings.TEMP_PATH + "/" + vMatrixPath);
			// Updating path of the current iteration matrix.
			vMatrixPath = "V_" + iteration;

		} else {
			System.out.println("SWERR: Unhandled matrix type. Cannot optimize");
			return;
		}

		// Path of the matrix M which contains userid/movieid -
		// normalized_ratings.
		matricesPaths[1] = new Path(Settings.TEMP_PATH + "/"
				+ Settings.NORMALIZE_DATA_PATH);

		// Set the input and output file paths for the current job
		// iteration which updates the matrix U or V depending on its
		// previous iteration matrix and updates the values to a new
		// iteration matrix.

		FileInputFormat.setInputPaths(conf2, matricesPaths);
		if (matrixType == Constants.U_Matrix) {
			FileOutputFormat.setOutputPath(conf2, new Path(Settings.TEMP_PATH
					+ "/" + uMatrixPath));
		} else if (matrixType == Constants.V_Matrix) {
			FileOutputFormat.setOutputPath(conf2, new Path(Settings.TEMP_PATH
					+ "/" + vMatrixPath));
		}

		// Start the Job.

		Job job2 = new Job(conf2);

		JobControl jobControl = new JobControl("jobControl");
		jobControl.addJob(job2);
		handleRun(jobControl);

	}

	public void initializeUV() {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			Path uFilePath = new Path(Settings.TEMP_PATH + "/U_0/U");
			Path vFilePath = new Path(Settings.TEMP_PATH + "/V_0/V");

			BufferedWriter br = null;

			// 2 a. Initialize U
			br = new BufferedWriter(new OutputStreamWriter(fs.create(uFilePath,
					true)));

			for (int i = 1; i <= Settings.noOfUsers; ++i)
				for (int j = 1; j <= Settings.noOfCommonFeatures; ++j) {
					br.write("U " + i + " " + j + " " + 1 + "\n");
				}

			br.close();

			// 2 b. Initialize V
			br = new BufferedWriter(new OutputStreamWriter(fs.create(vFilePath,
					true)));

			for (int i = 1; i <= Settings.noOfCommonFeatures; ++i)
				for (int j = 1; j <= Settings.noOfMovies; ++j) {
					br.write("V " + i + " " + j + " " + 1 + "\n");
				}

			br.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void normalizeM() throws IOException, InterruptedException {

		JobConf conf1 = new JobConf(UVDriver.class);
		conf1.setMapperClass(MPreMap.class);
		conf1.setReducerClass(MPreReduce.class);
		conf1.setJarByClass(UVDriver.class);

		conf1.setMapOutputKeyClass(Text.class);
		conf1.setMapOutputValueClass(Text.class);

		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(conf1, new Path(Settings.INPUT_PATH));
		FileOutputFormat.setOutputPath(conf1, new Path(Settings.TEMP_PATH + "/"
				+ Settings.NORMALIZE_DATA_PATH));

		Job job1 = new Job(conf1);

		JobControl jobControl = new JobControl("jobControl");
		jobControl.addJob(job1);
		handleRun(jobControl);

	}

	public static class Settings {

		public static final boolean BIG_DATA = false;

		public static String INPUT_SEPERATOR = "";
		public static int noOfUsers = 0;
		public static int noOfMovies = 0;

		public static final int noOfCommonFeatures = 10;
		public static final int noOfIterationsRequired = 2;

		public static final String NORMALIZE_DATA_PATH = "normalize";
		public static String INPUT_PATH = "input";
		public static String OUTPUT_PATH = "output";
		public static String TEMP_PATH = "temp";

		static {
			if (BIG_DATA) {
				INPUT_SEPERATOR = Constants.BIG_DATA_SEPERATOR;
				noOfUsers = Constants.BIG_DATA_USERS;
				noOfMovies = Constants.BIG_DATA_MOVIES;
			} else {
				INPUT_SEPERATOR = Constants.BIG_DATA_SEPERATOR;
				noOfUsers = Constants.SMALL_DATA_USERS;
				noOfMovies = Constants.SMALL_DATA_MOVIES;
			}
		}
	}

	public static class Constants {

		public static final String BIG_DATA_SEPERATOR = "::";
		public static final String SMALL_DATA_SEPERATOR = "\\s";

		public static final int BIG_DATA_USERS = 71567;
		public static final int BIG_DATA_MOVIES = 10681;
		public static final int SMALL_DATA_USERS = 943;
		public static final int SMALL_DATA_MOVIES = 1682;

		public static final int M_Matrix = 1;
		public static final int U_Matrix = 2;
		public static final int V_Matrix = 3;
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
			System.out.println("Waiting jobs: "
					+ control.getWaitingJobs().toString());
			System.out.println("Successful jobs: "
					+ control.getSuccessfulJobs().toString());
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

		Settings.INPUT_PATH = args[0];
		Settings.OUTPUT_PATH = args[1];
		Settings.TEMP_PATH = args[2];

		// 1. Pre-process the data.
		startTimer();
		normalizeM();
		stopTimer();

		System.out.println("Total time for Normalizing data: "
				+ getJobTimeInSecs() + "seconds");

		// 2. Initialize UV Matrices.
		startTimer();
		initializeUV();
		stopTimer();

		System.out.println("Total time for Initializing U and V: "
				+ getJobTimeInSecs() + "seconds");

		// 3. Iterate and update U, V matrices.
		for (int i = 1; i <= Settings.noOfIterationsRequired; i++) {

			startTimer();
			optimizeUVMatrix(Constants.U_Matrix, i);
			stopTimer();

			System.out
					.println("Total time for optimizing U Matrix in iteration: "
							+ i + " is: " + getJobTimeInSecs() + "seconds");

			startTimer();
			optimizeUVMatrix(Constants.V_Matrix, i);
			stopTimer();

			System.out
					.println("Total time for optimizing V Matrix in iteration: "
							+ i + " is: " + getJobTimeInSecs() + "seconds");

		}
		return 0;
	}

	public static void main(String args[]) throws Exception {

		System.out.println("Program started");
		if (args.length != 3) {
			System.err
					.println("Usage: UVDriver <input path> <output path> <fs path>");
			System.exit(-1);
		}

		Configuration configuration = new Configuration();
		String[] otherArgs = new GenericOptionsParser(configuration, args)
				.getRemainingArgs();
		ToolRunner.run(new UVDriver(), otherArgs);
		System.out.println("Program complete.");
		System.exit(0);
	}

	public void map(LongWritable arg0, Text arg1,
			OutputCollector<Text, Text> arg2, Reporter arg3) throws IOException {
		// TODO Auto-generated method stub

	}

}
