import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
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

	public void optimizeUVMatrix(int matrixType, int iteration)
			throws IOException, InterruptedException, URISyntaxException {
		JobConf conf3 = new JobConf(UVDriver.class);

		conf3.setOutputKeyClass(Text.class);
		conf3.setOutputValueClass(Text.class);

		// Set the paths of the previous iteration matrices
		// so that we can load them when needed during job.
		conf3.set("uPath", Settings.TEMP_PATH + "/" + uMatrixPath);
		conf3.set("vPath", Settings.TEMP_PATH + "/" + vMatrixPath);

		Path matricesPaths[] = new Path[2];

		if (matrixType == Constants.U_Matrix) {
			conf3.setMapperClass(UpdateUMapper.class);
			conf3.setReducerClass(UpdateUReducer.class);
			// Setting the path of the previous iteration matrix.
			matricesPaths[0] = new Path(Settings.TEMP_PATH + "/" + uMatrixPath);
			// Updating path of the current iteration matrix.
			uMatrixPath = "U_" + iteration;

		} else if (matrixType == Constants.V_Matrix) {
			conf3.setMapperClass(UpdateVMapper.class);
			conf3.setReducerClass(UpdateVReducer.class);
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

		FileInputFormat.setInputPaths(conf3, matricesPaths);
		if (matrixType == Constants.U_Matrix) {
			FileOutputFormat.setOutputPath(conf3, new Path(Settings.TEMP_PATH
					+ "/" + uMatrixPath));
		} else if (matrixType == Constants.V_Matrix) {
			FileOutputFormat.setOutputPath(conf3, new Path(Settings.TEMP_PATH
					+ "/" + vMatrixPath));
		}

		conf3.set("noOfCommonFeatures", Settings.noOfCommonFeatures.toString());

		// Start the Job.

		Job job3 = new Job(conf3);

		JobControl jobControl = new JobControl("jobControl");
		jobControl.addJob(job3);
		handleRun(jobControl);
	}

	public void initializeUV() {

		System.out.println("Initializing U and V..");
		// 2. Get the reasonable initial value.
		// We are simply choosing a constant value as we already normalized
		// the input matrix.

		try {

			JobConf conf1 = new JobConf(UVDriver.class);
			conf1.setMapperClass(InitializeUMapper.class);
			conf1.setReducerClass(InitializeUReducer.class);
			conf1.setJarByClass(UVDriver.class);

			conf1.setMapOutputKeyClass(Text.class);
			conf1.setMapOutputValueClass(Text.class);

			conf1.setOutputKeyClass(Text.class);
			conf1.setOutputValueClass(Text.class);

			conf1.setInputFormat(TextInputFormat.class);
			conf1.setOutputFormat(TextOutputFormat.class);

			conf1.set("BIG_DATA", Settings.BIG_DATA.toString());
			conf1.set("noOfCommonFeatures",
					Settings.noOfCommonFeatures.toString());
			conf1.set("INITIAL_VALUE", Settings.INITIAL_VALUE.toString());

			FileInputFormat.addInputPath(conf1, new Path(Settings.INPUT_PATH));
			FileOutputFormat.setOutputPath(conf1, new Path(Settings.TEMP_PATH
					+ "/U_0"));

			JobConf conf2 = new JobConf(UVDriver.class);
			conf2.setMapperClass(InitializeVMapper.class);
			conf2.setReducerClass(InitializeVReducer.class);
			conf2.setJarByClass(UVDriver.class);

			conf2.setMapOutputKeyClass(Text.class);
			conf2.setMapOutputValueClass(Text.class);

			conf2.setOutputKeyClass(Text.class);
			conf2.setOutputValueClass(Text.class);

			conf2.setInputFormat(TextInputFormat.class);
			conf2.setOutputFormat(TextOutputFormat.class);

			conf2.set("BIG_DATA", Settings.BIG_DATA.toString());
			conf2.set("noOfCommonFeatures",
					Settings.noOfCommonFeatures.toString());
			conf2.set("INITIAL_VALUE", Settings.INITIAL_VALUE.toString());

			FileInputFormat.addInputPath(conf2, new Path(Settings.INPUT_PATH));
			FileOutputFormat.setOutputPath(conf2, new Path(Settings.TEMP_PATH
					+ "/V_0"));

			Job job1 = new Job(conf1);
			Job job2 = new Job(conf2);

			JobControl jobControl = new JobControl("jobControl");
			jobControl.addJob(job1);
			jobControl.addJob(job2);
			job2.addDependingJob(job1);
			handleRun(jobControl);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void normalizeM() throws IOException, InterruptedException {

		System.out.println("Normalizing..");
		JobConf conf1 = new JobConf(UVDriver.class);
		conf1.setMapperClass(RowMPreMap.class);
		conf1.setReducerClass(RowMPreReduce.class);
		conf1.setJarByClass(UVDriver.class);

		conf1.setMapOutputKeyClass(Text.class);
		conf1.setMapOutputValueClass(Text.class);

		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(Text.class);

		conf1.setKeepFailedTaskFiles(true);

		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);

		conf1.set("BIG_DATA", Settings.BIG_DATA.toString());

		FileInputFormat.addInputPath(conf1, new Path(Settings.INPUT_PATH));
		FileOutputFormat.setOutputPath(conf1, new Path(Settings.TEMP_PATH + "/"
				+ Settings.NORMALIZE_DATA_PATH_TEMP));

		JobConf conf2 = new JobConf(UVDriver.class);
		conf2.setMapperClass(ColMPreMap.class);
		conf2.setReducerClass(ColMPreReduce.class);
		conf2.setJarByClass(UVDriver.class);

		conf2.setMapOutputKeyClass(Text.class);
		conf2.setMapOutputValueClass(Text.class);

		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(conf2, new Path(Settings.TEMP_PATH + "/"
				+ Settings.NORMALIZE_DATA_PATH_TEMP));
		FileOutputFormat.setOutputPath(conf2, new Path(Settings.TEMP_PATH + "/"
				+ Settings.NORMALIZE_DATA_PATH));

		Job job1 = new Job(conf1);
		Job job2 = new Job(conf2);

		JobControl jobControl = new JobControl("jobControl");
		jobControl.addJob(job1);
		jobControl.addJob(job2);
		job2.addDependingJob(job1);
		handleRun(jobControl);

	}

	public static class Settings {

		// These static variables will not be seen properly in
		// distributed mode. Use them carefully. Always pass them
		// by setting in the configuration object.
		public static Boolean BIG_DATA = false;

		public static int noOfUsers = 0;
		public static int noOfMovies = 0;

		public static final Integer noOfCommonFeatures = 10;
		public static final int noOfIterationsRequired = 30;
		public static final Float INITIAL_VALUE = 0.1f;

		public static final String NORMALIZE_DATA_PATH_TEMP = "normalize_temp";
		public static final String NORMALIZE_DATA_PATH = "normalize";
		public static String INPUT_PATH = "input";
		public static String OUTPUT_PATH = "output";
		public static String TEMP_PATH = "temp";

	}

	public static class Constants {

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

		int i = 0;
		while (!control.allFinished()) {
			if (i % 20 == 0) {
				System.out
						.println(new Date().toString() + ": Still running...");
				System.out.println("Running jobs: "
						+ control.getRunningJobs().toString());
				System.out.println("Waiting jobs: "
						+ control.getWaitingJobs().toString());
				System.out.println("Successful jobs: "
						+ control.getSuccessfulJobs().toString());
			}
			Thread.sleep(1000);
			i++;
		}

		if (control.getFailedJobs() != null) {
			System.out.println("Failed jobs: "
					+ control.getFailedJobs().toString());
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
		Settings.BIG_DATA = Boolean.parseBoolean(args[3]);

		// Setting below variables doesn't mean you can use them in mapper
		// or reducers. You are not allowed to access them in distributed
		// methods. Pass them by JobConf if required.
		if (Settings.BIG_DATA) {
			System.out.println("Working on BIG DATA.");
			Settings.noOfUsers = Constants.BIG_DATA_USERS;
			Settings.noOfMovies = Constants.BIG_DATA_MOVIES;
		} else {
			System.out.println("Working on Small DATA.");
			Settings.noOfUsers = Constants.SMALL_DATA_USERS;
			Settings.noOfMovies = Constants.SMALL_DATA_MOVIES;
		}

		// 1. Pre-process the data.
		startTimer();
		normalizeM();
		stopTimer();

		printline();
		System.out.println("Total time for Normalizing data: "
				+ getJobTimeInSecs() + "seconds");

		// 2. Initialize UV Matrices.
		startTimer();
		initializeUV();
		stopTimer();

		printline();
		System.out.println("Total time for Initializing U and V: "
				+ getJobTimeInSecs() + "seconds");

		printline();
		// 3. Iterate and update U, V matrices.
		for (int i = 1; i <= Settings.noOfIterationsRequired; i++) {

			startTimer();
			optimizeUVMatrix(Constants.U_Matrix, i);
			stopTimer();

			printline();
			System.out
					.println("Total time for optimizing U Matrix in iteration: "
							+ i + " is: " + getJobTimeInSecs() + "seconds");

			startTimer();
			optimizeUVMatrix(Constants.V_Matrix, i);
			stopTimer();

			printline();
			System.out
					.println("Total time for optimizing V Matrix in iteration: "
							+ i + " is: " + getJobTimeInSecs() + "seconds");

		}
		return 0;
	}

	private void printline() {
		System.out.print("\n");
		for (int i = 0; i < 80; i++) {
			System.out.print("=");
		}
		System.out.print("\n");
		System.out.flush();
	}

	public static void main(String args[]) throws Exception {

		System.out.println("Program started");
		if (args.length != 4) {
			System.err
					.println("Usage: UVDriver <input path> <output path> <temp path> <big_data_bool>");
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
