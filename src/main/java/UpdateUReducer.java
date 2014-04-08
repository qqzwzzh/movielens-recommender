import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class UpdateUReducer extends MapReduceBase implements
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
				if (new File(f.getPath().toString()).getName().startsWith("_"))
					continue;
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(f.getPath())));
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
				productUV.set(j, productUV.get(j) + vFeatureHm.get(movieID)[i]
						* (updatedFeatureValue - uFeature[i]));
			}

			uFeature[i] = updatedFeatureValue;
		}

		// Output: (U userId featureIndex featureValue)

		for (int i = 0; i < noOfCommonFeatures; i++) {
			output.collect(null, new Text("U " + key.toString() + " " + (i + 1)
					+ " " + uFeature[i]));
		}

	}

}
