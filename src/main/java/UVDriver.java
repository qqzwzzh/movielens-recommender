import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
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

	public static class RowMPreMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Text keyText = new Text();
		private Text valText = new Text();
		private static Boolean BIG_DATA;

		public void configure(JobConf conf) {
			String var2String = conf.get("BIG_DATA");
			BIG_DATA = Boolean.parseBoolean(var2String);
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Input: (lineNo, lineContent)

			// Split each line using seperator based on the dataset.
			String line[] = null;

			if (BIG_DATA)
				line = value.toString().split("::");
			else
				line = value.toString().split("\\s");

			keyText.set(line[0]);
			valText.set(line[1] + "," + line[2]);

			// Output: (userid, "movieid,rating")
			output.collect(keyText, valText);

		}
	}

	public static class RowMPreReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		private Text valText = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			try {

				// Input: (userid, List<movieid, rating>)

				float sum = 0.0F;
				int totalRatingCount = 0;

				ArrayList<String> movieID = new ArrayList<String>();
				ArrayList<Float> rating = new ArrayList<Float>();

				while (values.hasNext()) {
					String[] movieRatingPair = values.next().toString()
							.split(",");
					movieID.add(movieRatingPair[0]);
					Float parseRating = Float.parseFloat(movieRatingPair[1]);
					rating.add(parseRating);

					sum += parseRating;
					totalRatingCount++;
				}

				float average = ((float) sum) / totalRatingCount;

				for (int i = 0; i < movieID.size(); i++) {
					valText.set("M " + key.toString() + " " + movieID.get(i)
							+ " " + (rating.get(i) - average));
					output.collect(null, valText);
				}

			} catch (Exception e) {
				// A way to debug run time exceptions when you dont have
				// access to cluster logs.
				String valueString = "";
				while (values.hasNext()) {
					valueString += values.next().toString();
				}

				StringWriter sw = new StringWriter();
				e.printStackTrace(new PrintWriter(sw));
				String exceptionAsString = sw.toString();

				Path pt = new Path("errorfile");
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
						fs.create(pt, true)));
				br.write(exceptionAsString + "\nkey: " + key.toString()
						+ "\nvalues: " + valueString);
				br.close();
			}

			// Output: (null, <M userid, movieid, normalizedrating>)
		}
	}

	public static class ColMPreMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Text keyText = new Text();
		private Text valText = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Input: (lineNo, lineContent)
			// Split each line using seperator based on the dataset.

			String line[] = null;

			line = value.toString().split("\\s");

			keyText.set(line[2]);
			valText.set(line[1] + "," + line[3]);

			// Output: (userid, "movieid,rating")
			output.collect(keyText, valText);
		}
	}

	public static class ColMPreReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		private Text valText = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Input: (movieid, List<userid, rating>)

			float sum = 0.0F;
			int totalRatingCount = 0;

			ArrayList<String> userID = new ArrayList<String>();
			ArrayList<Float> rating = new ArrayList<Float>();

			while (values.hasNext()) {
				String[] userRatingPair = values.next().toString().split(",");
				userID.add(userRatingPair[0]);
				Float parseRating = Float.parseFloat(userRatingPair[1]);
				rating.add(parseRating);

				sum += parseRating;
				totalRatingCount++;
			}

			float average = ((float) sum) / totalRatingCount;

			for (int i = 0; i < userID.size(); i++) {
				valText.set("M " + userID.get(i) + " " + key.toString() + " "
						+ (rating.get(i) - average));
				output.collect(null, valText);
			}

			// Output1: (null, <M userid, movieid, normalizedrating>)

		}
	}

	public static class InitializeUMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private static Boolean BIG_DATA;

		public void configure(JobConf conf) {
			String var2String = conf.get("BIG_DATA");
			BIG_DATA = Boolean.parseBoolean(var2String);
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Input: (lineNo, lineContent)

			// Split each line using seperator based on the dataset.
			String line[] = null;

			if (BIG_DATA)
				line = value.toString().split("::");
			else
				line = value.toString().split("\\s");

			// Output: (U userid, null)
			output.collect(new Text(line[0]), new Text("1"));
		}
	}

	public static class InitializeUReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		private static int noOfCommonFeatures;
		private static float initialValue;

		public void configure(JobConf conf) {

			String var2String = conf.get("noOfCommonFeatures");
			noOfCommonFeatures = Integer.parseInt(var2String);
			var2String = conf.get("INITIAL_VALUE");
			initialValue = Float.parseFloat(var2String);
		}

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Input: (userid, nullList)
			for (int i = 1; i <= noOfCommonFeatures; i++) {

				// Output1: (U userid featureID, featureVal)
				output.collect(null, new Text("U " + key.toString() + " " + i
						+ " " + initialValue));

			}
		}

	}

	public static class InitializeVMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private static Boolean BIG_DATA;

		public void configure(JobConf conf) {
			String var2String = conf.get("BIG_DATA");
			BIG_DATA = Boolean.parseBoolean(var2String);
			var2String = conf.get("noOfCommonFeatures");
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Input: (lineNo, lineContent)

			// Split each line using seperator based on the dataset.
			String line[] = null;

			if (BIG_DATA)
				line = value.toString().split("::");
			else
				line = value.toString().split("\\s");

			// Output: (movieid, null)
			output.collect(new Text(line[1]), new Text("1"));

		}
	}

	public static class InitializeVReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		private static int noOfCommonFeatures;
		private static float initialValue;

		public void configure(JobConf conf) {
			String var2String = conf.get("noOfCommonFeatures");
			noOfCommonFeatures = Integer.parseInt(var2String);
			var2String = conf.get("INITIAL_VALUE");
			initialValue = Float.parseFloat(var2String);
		}

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Input: (movieid, nullList)
			for (int i = 1; i <= noOfCommonFeatures; i++) {

				// Output2: (V featureID, movieID, featureVal)
				output.collect(null, new Text("V " + i + " " + key.toString()
						+ " " + initialValue));

			}
		}
	}

	public static class UpdateUMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Input: (M/U userid, movieid/featureid, rating)
			String[] line = value.toString().split("\\s");
			output.collect(new Text(line[1]), value);
		}

	}

	public static class UpdateUReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		HashMap<Integer, Float[]> vFeatureHm = new HashMap<Integer, Float[]>();
		private int noOfCommonFeatures;
		private static BufferedWriter br;

		private void setup() throws IOException {
			// A way to debug run time exceptions when you dont have
			// access to cluster logs.
			Path pt = new Path("errorfile");
			FileSystem fs;

			fs = FileSystem.get(new Configuration());
			br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
		}

		public void close() throws IOException {
			br.close();
		}

		public void configure(JobConf job) {
			System.out.println("Configuring UpdateUReducer");

			String commonFeaturesStr = job.get("noOfCommonFeatures");
			noOfCommonFeatures = Integer.parseInt(commonFeaturesStr);

			String vPath = job.get("vPath");
			System.out.println("VPATH: " + vPath);
			loadVMatrix(new Path(vPath));

			try {
				setup();
			} catch (IOException e) {
				e.printStackTrace();
			}

			try {
				br.write("No. of features: " + noOfCommonFeatures);
			} catch (IOException e1) {
				e1.printStackTrace();
			}

		}

		private void loadVMatrix(Path cachePath) {
			System.out.println("Loading V Matrix..");

			try {
				FileSystem fs = FileSystem.get(new Configuration());

				FileStatus[] files = fs.listStatus(cachePath);
				for (FileStatus f : files) {
					// If that is a temp file, ingore it.
					if (new File(f.getPath().toString()).getName().startsWith(
							"_"))
						continue;
					BufferedReader br = new BufferedReader(
							new InputStreamReader(fs.open(f.getPath())));
					String line;

					// Input: (V featureID movieID featureValue)
					while ((line = br.readLine()) != null) {
						String[] tokens = line.split("\\s");
						int movieid = Integer.parseInt(tokens[2]);
						String featureIndex = tokens[1];
						int fi = Integer.parseInt(featureIndex);
						String featureValue = tokens[3];
						float fv = Float.parseFloat(featureValue);

						// Storing each feature value against movieID and
						// featureIndex.
						// movieID is not sequential and featureIndex starts
						// from 0.
						// So we store at fi - 1.
						Float[] featureValues = vFeatureHm.get(movieid);
						if (featureValues == null) {
							featureValues = new Float[noOfCommonFeatures];
							featureValues[fi - 1] = fv;
							vFeatureHm.put(movieid, featureValues);
						} else {
							featureValues[fi - 1] = fv;
						}
					}

					br.close();
				}

			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("SWERR: File error.");
			}
		}

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Input: (userid, <list of M/U entries with that userid in format
			// M/U userid movieid/featureid value>)

			ArrayList<Integer> movieIDs = new ArrayList<Integer>();
			ArrayList<Float> ratings = new ArrayList<Float>();
			ArrayList<Float> productUV = new ArrayList<Float>();

			float[] uFeature = new float[10];

			while (values.hasNext()) {
				String line[] = values.next().toString().split("\\s");

				if (line[0].equals("M")) {

					// Store all Movie IDs and ratings into lists. We will use
					// them later for computation based on the index.

					movieIDs.add(Integer.parseInt(line[2]));
					ratings.add(Float.parseFloat(line[3]));

					// Initialize the productUV to 0.
					productUV.add(0f);

				} else if (line[0].equals("U")) {

					// Store the feature values against their index.
					// Since the U matrix store FI starting with 1, do -1 here.
					uFeature[Integer.parseInt(line[2]) - 1] = Float
							.parseFloat(line[3]);

				} else {
					System.out.println("SWERR: Invalid record.");
				}
			}

			// Compute the new feature values for the current user row in U.

			// Update the productUV
			// For each user vector (row), we are multiplying with corresponding
			// vector in V matrix which is the movie vector (column)
			for (int i = 0; i < movieIDs.size(); i++) {
				int movieID = movieIDs.get(i);
				float sum = 0;
				for (int j = 0; j < noOfCommonFeatures; j++) {
					sum += uFeature[j] * vFeatureHm.get(movieID)[j];
				}
				productUV.set(i, sum);
			}

			// Update the feature values

			// Pick a random permutation of feature vector to start the update.
			ArrayList<Integer> featureIndex = new ArrayList<Integer>(10);
			for (int i = 0; i < noOfCommonFeatures; i++)
				featureIndex.add(i);
			Collections.shuffle(featureIndex);

			// Start to process features as per the permutation order.
			for (int featureX : featureIndex) {

				// The actual featureIndex we are processing now.
				int i = featureIndex.get(featureX);

				// For each movieID

				float innerProduct = 0;
				float viSquare = 0;
				for (int j = 0; j < movieIDs.size(); j++) {
					int movieID = movieIDs.get(j);
					float subtract = ratings.get(j) - productUV.get(j)
							+ uFeature[i] * vFeatureHm.get(movieID)[i];
					innerProduct += subtract * vFeatureHm.get(movieID)[i];
					viSquare += vFeatureHm.get(movieID)[i]
							* vFeatureHm.get(movieID)[i];
				}

				float updatedFeatureValue = (float) innerProduct / viSquare;

				for (int j = 0; j < movieIDs.size(); j++) {
					int movieID = movieIDs.get(j);

					// Subtract old feature contribution and add new
					// feature contribution.
					productUV.set(j, productUV.get(j)
							+ vFeatureHm.get(movieID)[i]
							* (updatedFeatureValue - uFeature[i]));
				}

				uFeature[i] = updatedFeatureValue;
			}

			// Output: (U userId featureIndex featureValue)

			for (int i = 0; i < noOfCommonFeatures; i++) {
				output.collect(null, new Text("U " + key.toString() + " "
						+ (i + 1) + " " + uFeature[i]));
			}

		}

	}

	public static class UpdateVMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Input: (M/V userid/featureid movieid rating)
			String[] line = value.toString().split("\\s");

			// Output: (movieid, <value>)
			output.collect(new Text(line[2]), value);
		}

	}

	public static class UpdateVReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		HashMap<Integer, Float[]> uFeatureHm = new HashMap<Integer, Float[]>();
		private static int noOfCommonFeatures;

		public void configure(JobConf job) {
			System.out.println("Configuring UpdateUReducer");

			String commonFeaturesStr = job.get("noOfCommonFeatures");
			noOfCommonFeatures = Integer.parseInt(commonFeaturesStr);

			String uPath = job.get("uPath");
			System.out.println("UPATH: " + uPath);
			loadUMatrix(new Path(uPath));

		}

		private void loadUMatrix(Path cachePath) {
			System.out.println("Loading V Matrix..");

			try {
				FileSystem fs = FileSystem.get(new Configuration());

				FileStatus[] files = fs.listStatus(cachePath);
				for (FileStatus f : files) {
					// If that is a temp file, ingore it.
					if (new File(f.getPath().toString()).getName().startsWith(
							"_"))
						continue;
					BufferedReader br = new BufferedReader(
							new InputStreamReader(fs.open(f.getPath())));
					String line;

					// Input: (U UserID featureIndex featureValue)
					while ((line = br.readLine()) != null) {
						String[] tokens = line.split("\\s");
						int userid = Integer.parseInt(tokens[1]);
						String featureIndex = tokens[2];
						int fi = Integer.parseInt(featureIndex);
						String featureValue = tokens[3];
						float fv = Float.parseFloat(featureValue);

						// Storing each feature value against movieID and
						// featureIndex.
						// movieID starts from 1 and featureIndex starts from 0.
						// So we store at fi - 1.
						Float[] featureValues = uFeatureHm.get(userid);
						if (featureValues == null) {
							featureValues = new Float[noOfCommonFeatures];
							featureValues[fi - 1] = fv;
							uFeatureHm.put(userid, featureValues);
						} else {
							featureValues[fi - 1] = fv;
						}
					}

					br.close();
				}

			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("SWERR: File error.");
			}
		}

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Input: (movieID, <list of M/V entries with that movieID in format
			// M/V userid/featureid movieid value>)

			ArrayList<Integer> userIDs = new ArrayList<Integer>();
			ArrayList<Float> ratings = new ArrayList<Float>();
			ArrayList<Float> productUV = new ArrayList<Float>();

			float[] vFeature = new float[10];

			while (values.hasNext()) {
				String line[] = values.next().toString().split("\\s");

				if (line[0].equals("M")) {

					// Store all Movie IDs and ratings into lists. We will use
					// them later for computation based on the index.

					userIDs.add(Integer.parseInt(line[1]));
					ratings.add(Float.parseFloat(line[3]));

					// Initialize the productUV to 0.
					productUV.add(0f);

				} else if (line[0].equals("V")) {

					// Store the feature values against their index.
					// Since the U matrix store FI starting with 1, do -1 here.
					vFeature[Integer.parseInt(line[1]) - 1] = Float
							.parseFloat(line[3]);

				} else {
					System.out.println("SWERR: Invalid record.");
				}
			}

			// Compute the new feature values for the current movie row in V.

			// Update the productUV
			// For each movie vector, we are multiplying with corresponding
			// vector in U matrix which is the user vector
			for (int i = 0; i < userIDs.size(); i++) {
				int userID = userIDs.get(i);
				float sum = 0;
				for (int j = 0; j < noOfCommonFeatures; j++) {
					sum += vFeature[j] * uFeatureHm.get(userID)[j];
				}
				productUV.set(i, sum);
			}

			// Update the feature values

			// Pick a random permutation of feature vector to start the update.
			ArrayList<Integer> featureIndex = new ArrayList<Integer>(10);
			for (int i = 0; i < noOfCommonFeatures; i++)
				featureIndex.add(i);
			Collections.shuffle(featureIndex);

			// Start to process features as per the permutation order.
			for (int featureX : featureIndex) {

				// The actual featureIndex we are processing now.
				int i = featureIndex.get(featureX);

				// For each userID

				float innerProduct = 0;
				float ujSquare = 0;
				for (int j = 0; j < userIDs.size(); j++) {
					int userID = userIDs.get(j);
					float subtract = ratings.get(j) - productUV.get(j)
							+ vFeature[i] * uFeatureHm.get(userID)[i];
					innerProduct += subtract * uFeatureHm.get(userID)[i];
					ujSquare += uFeatureHm.get(userID)[i]
							* uFeatureHm.get(userID)[i];
				}

				float updatedFeatureValue = (float) innerProduct / ujSquare;

				for (int j = 0; j < userIDs.size(); j++) {
					int userID = userIDs.get(j);

					// Subtract old feature contribution and add new
					// feature contribution.
					productUV.set(j, productUV.get(j)
							+ uFeatureHm.get(userID)[i]
							* (updatedFeatureValue - vFeature[i]));
				}

				vFeature[i] = updatedFeatureValue;
			}

			// Output: (U userId featureIndex featureValue)

			for (int i = 0; i < noOfCommonFeatures; i++) {
				output.collect(null,
						new Text("V " + (i + 1) + " " + key.toString() + " "
								+ vFeature[i]));
			}

		}

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
			conf1.set("noOfCommonFeatures", Settings.noOfCommonFeatures.toString());
			conf1.set("INITIAL_VALUE", Settings.INITIAL_VALUE.toString());
			
			FileInputFormat.addInputPath(conf1, new Path(Settings.INPUT_PATH));
			FileOutputFormat.setOutputPath(conf1, new Path(Settings.TEMP_PATH + "/U_0"));

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
			conf2.set("noOfCommonFeatures", Settings.noOfCommonFeatures.toString());
			conf2.set("INITIAL_VALUE", Settings.INITIAL_VALUE.toString());
			
			FileInputFormat.addInputPath(conf2, new Path(Settings.INPUT_PATH));
			FileOutputFormat.setOutputPath(conf2, new Path(Settings.TEMP_PATH + "/V_0"));

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
		public static final int noOfIterationsRequired = 10;
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
	
	private void printline(){
		System.out.print("\n");	
		for(int i = 0; i < 80; i++){
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
